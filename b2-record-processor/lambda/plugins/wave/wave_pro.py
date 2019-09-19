import json

import datamodule.farm_model as farm
import datamodule.phase_model as phase
import plugins.wave.wave_schemas as wave_schemas
from config import SYS_CLOUD_PROVIDER
from helpers import UNDEFINED, cast_to_int
from models.data_lake_record import DataLakeRecord
from models.record_processor import RecordProcessorBase

ob_phase = phase.PhaseModel()
ob_farm = farm.FarmModel()


class WaveRecordProcessor(RecordProcessorBase):

    _capture_local_datetime = None
    _capture_timestamp = None
    _direction = None
    _distance_cm = None
    _height_cm = None
    _sensor_recipe = None
    _tag_id = None
    _row_session_id = None
    _wave_filename = None

    @staticmethod
    def types_to_be_processed():
        return ["wave"]

    def on_line_output_exception(self, input_line: dict, payload_count: int, error: Exception):
        super().on_line_output_exception(input_line, payload_count, error)
        raise error

    def on_process_stage_files_error(self, filepath, error):
        super().on_process_stage_files_error(filepath, error)

    def create_output_payload(self, input_stage_file, filepath):
        self.set_wave_filename(filepath=filepath)
        payload_count = 0
        self.current_payload = []
        for json_line in input_stage_file:
            self.current_payload.append(json_line)
            payload_count += 1

        self.set_tag_id()

        for json_line in self.current_payload:
            self.set_row_session_id(json_line=json_line)
            self.set_capture_timestamp(json_line=json_line)
            self.set_capture_local_datetime(json_line=json_line)
            self.set_distance_cm(json_line=json_line)
            self.set_height_cm(json_line=json_line)
            self.set_direction(json_line=json_line)
            self.set_sensor_recipe(json_line=json_line)

        output_count = 0
        for output in self.create_message_payload(self.current_payload):
            output_count += 1
            self.write_to_stage(output)

        return payload_count

    def get_wave_filename(self):
        return self._wave_filename

    def set_wave_filename(self, filepath):
        self._wave_filename = filepath.split('__')[1]

    def create_message_payload(self, payload):
        for json_line in payload:
            if json_line['id'] in ['wave-info', 'wave']:
                record = WaveDatalakeRecord(record_processor=self)
                yield record.get_content(json_line)

    def get_direction(self):
        if not self._direction:
            raise ValueError("'direction' is not defined. id='device-id' was not found in the wave file.")
        return self._direction

    def set_direction(self, json_line):
        if json_line["id"] == "device-id":
            direction = json_line["instance"]
            if direction == '0':
                direction = 'left'
            elif direction == '1':
                direction = 'right'
            self._direction = direction

    def get_height_cm(self):
        return self._height_cm

    def set_height_cm(self, json_line):
        if json_line["id"] == "location":
            self._height_cm = json_line["height"]

    def get_distance_cm(self):
        return self._distance_cm

    def set_distance_cm(self, json_line):
        if json_line["id"] == "location":
            self._distance_cm = json_line["distance"]

    def set_row_session_id(self, json_line):
        if json_line["id"] == "location":
            self._row_session_id = json_line.get("rsid", UNDEFINED)

    def get_tag_id(self):
        return self._tag_id

    def set_tag_id(self):
        for json_line in self.current_payload:
            if json_line["id"] == "location":
                self._tag_id = json_line["tagid"]
            if self.tag_id:
                break

    def get_capture_timestamp(self):
        # TODO: review cast to int
        return cast_to_int(self._capture_timestamp)

    def set_capture_timestamp(self, json_line):
        if (json_line.get('id', None) == 'samplingTime') and json_line.get('param', None) == 'unix':
            self._capture_timestamp = json_line.get('value', None)

    def get_capture_local_datetime(self):
        return self._capture_local_datetime

    def set_capture_local_datetime(self, json_line):
        if json_line['id'] == 'samplingTime' and json_line['param'] == 'utc':
            sampling_time_utc = json_line['value']
            self._capture_local_datetime = self.convert_to_farm_timezone(sampling_time_utc)

    def get_row_session_id(self):
        return self._row_session_id

    def get_sensor_recipe(self):
        return self._sensor_recipe

    def set_sensor_recipe(self, json_line):
        if json_line['id'] == 'cfg':
            self._sensor_recipe = json_line.copy()

    def get_velocity(self):
        return 0

    capture_local_datetime = property(get_capture_local_datetime)
    capture_timestamp = property(get_capture_timestamp)
    direction = property(get_direction)
    distance_cm = property(get_distance_cm)
    height_cm = property(get_height_cm)
    row_session_id = property(get_row_session_id)
    sensor_recipe = property(get_sensor_recipe)
    tag_id = property(get_tag_id)
    velocity = property(get_velocity)


class WaveDatalakeRecord(DataLakeRecord):

    def get_capture_timestamp(self):
        return self.record_processor.capture_timestamp

    def get_distance_cm(self):
        return self.record_processor.distance_cm

    def get_velocity(self):
        return self.record_processor.velocity

    def get_height_cm(self):
        return self.record_processor.height_cm

    def get_record_type(self):
        return f"{self.record_processor.record_type}/{self.json_id}"

    def get_direction(self):
        return self.record_processor.direction

    def get_json_schema(self):
        return wave_schemas.wave_json_schema['wave_data_payload']

    def get_sensor_data(self):
        sensor_data = None
        if self.is_id_wave_info:
            sensor_data = self.json_line['wavelength']
        elif self.is_id_wave:
            sensor_data = self.json_line['value']
        return sensor_data

    def get_extra_content(self):
        return {
            "wave_file_path": self.get_wave_file_path(),
            "sensor_recipe": self.record_processor.get_sensor_recipe(),
            "sensor_meta": self.get_sensor_meta(),
            "sensor_data": self.get_sensor_data(),
        }

    def get_json_id(self):
        return self.json_line['id']

    def is_id_wave_info(self):
        return self.json_id == 'wave-info'

    def is_id_wave(self):
        return self.json_id == 'wave'

    def get_wave_file_path(self):
        return self.record_processor.get_wave_filename()

    def get_sensor_meta(self):
        sensor_meta = self.json_line.copy()

        if self.is_id_wave_info:
            if "wavelength" in sensor_meta:
                sensor_meta.pop('wavelength')

        elif self.is_id_wave:
            if "value" in sensor_meta:
                sensor_meta.pop('value')

        return sensor_meta

    json_id = property(get_json_id)
    is_id_wave_info = property(is_id_wave_info)
    is_id_wave = property(is_id_wave)
    wave_file_path = property(get_wave_file_path)
    sensor_meta = property(get_sensor_meta)


if __name__ == "__main__":
    wave_event = {
        "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": "2019-04-05 18:51:48",
            "sequenceNumber": "49594210671634229205053591547873906933834475997243637794",
            "data": "ewogICJ2ZXJzaW9uIjogMS4wLAogICJzb3VyY2UiOiAid2F2ZS8xMjM0NTY3OC0wOTgwLTI0MDAtMDAwOS0xMDAzMDAwNTAwMDQvMjAxOS0wNC0wMS80MDAwMDAxMDEwNkZDNC0yMDE5MDQwMS0wLTAwMDAwMC50YXIuYnoyIiwKICAidmFsaWQiOiB0cnVlLAogICJpbmNvbWluZ190aW1lc3RhbXAiOiAiMTk3MC0wMS0wMSAwMDowMDowMCIsCiAgIm91dGdvaW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA0LTA1IDE4OjUxOjQ4IiwKICAicGF5bG9hZCI6IFsicmF3LXByb2Nlc3Nvci9wcm9jZXNzX2RhdGU9MjAxOS0wNS0yMS9kYXRhdHlwZT13YXZlL3Byb2Nlc3NfaWQ9NDc3ZjJiOTQtN2MyMS0xMWU5LTkxYzktMzA5YzIzZTU4MTA4L3Jhdy1wcm9jZXNzb3JfXzAwMDAwMDAxODIxNzYzNDQtMjAxOTA0MDItMC0wMDAwMDAudGFyLmJ6Ml9fMDAwMDAwMDE4MjE3NjM0NC0yMDE5MDQwMi0wLTIyMjI0Ny53YXZlX180NzdmMmI5NC03YzIxLTExZTktOTFjOS0zMDljMjNlNTgxMDguY3N2Lmd6Il0sCiAgInR5cGUiOiAid2F2ZSIsCiAgImZpbGVuYW1lIjogIjQwMDAwMDEwMTA2RkM0LTIwMTkwNDAxLTAtMTU1MzI3LndhdmUiCn0=",
            "approximateArrivalTimestamp": 1554490308.545
        },
        "eventSource": "aws:kinesis",
        "eventVersion": "1.0",
        "eventID": "shardId-000000000002:49594210671634229205053591547873906933834475997243637794",
        "eventName": "aws:kinesis:record",
        "invokeIdentityArn": "arn:aws:iam::425695594515:role/lambda-kinesis-role",
        "awsRegion": "us-west-2",
        "eventSourceARN": "arn:aws:kinesis:us-west-2:425695594515:stream/kinesis-RawDataStream-1R0K6AS5TFUIH"
    }

    rp = WaveRecordProcessor(name="wave_pro", cloud_provider=SYS_CLOUD_PROVIDER)
    result = rp.process(wave_event)
    print(json.dumps(result, indent=4))
