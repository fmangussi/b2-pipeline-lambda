from helpers import UNDEFINED, cast_to_int
from models.data_lake_record import DataLakeRecord

import plugins.aux.aux_schemas as aux_schemas

from config import SYS_CLOUD_PROVIDER

from models.record_processor import RecordProcessorBase


class AuxRecordProcessor(RecordProcessorBase):

    @staticmethod
    def types_to_be_processed():
        return ["aux"]

    def create_message_payload(self, input_line: dict):
        record = AuxDatalakeRecord(record_processor=self)
        yield record.get_content(input_line)

    def get_tag_id(self):
        return self.current_payload["location"]["tagid"]

    tag_id = property(get_tag_id)


class AuxDatalakeRecord(DataLakeRecord):

    def get_json_schema(self):
        return aux_schemas.aux_json_schema["aux_data_payload"]

    def get_capture_timestamp(self):
        # TODO: review cast to int
        return cast_to_int(self.json_line["time"])

    def get_distance_cm(self):
        #print("self.json_line", self.json_line)
        return self.json_line["location"]["distance"]

    def get_velocity(self):
        return self.json_line['location']['velocity']

    def get_height_cm(self):
        return self.json_line['location']['height']

    def get_record_type(self):
        return self.record_processor.get_record_type() + self.json_line['namespace']

    def get_direction(self):
        return UNDEFINED

    def get_sensor_meta(self):
        non_meta_keys = ['location','time',self.json_line['id']]
        sensor_meta = self.json_line.copy()
        for key in non_meta_keys:
            if key in sensor_meta:
                sensor_meta.pop(key)
        
        return sensor_meta

    def get_sensor_data(self):
        non_sensor_keys = ["location","time","id","namespace","node","seg"]
        sensor_data = self.json_line.copy()        
        for key in non_sensor_keys:
            if key in sensor_data:
                sensor_data.pop(key)
        return sensor_data

    def get_extra_content(self):
        return {
            "sensor_meta": self.get_sensor_meta(),
            "sensor_data": self.get_sensor_data()
        }


if __name__ == "__main__":
    video_event = {
      "Records": [
        {
          "kinesis": {
            "COMMENT": "aux",
            "kinesisSchemaVersion": "1.0",
            "partitionKey": "2019-04-24 01:07:43",
            "sequenceNumber": "49594210671589627714656895521215685727132014026751475714",
            "data": "ewogICJ2ZXJzaW9uIjogMS4wLAogICJzb3VyY2UiOiAiYXV4Lzg5NGY5NDQyOGE1NjJiYTdmNTI0ZjQwYjYyODAyNDcyLzIwMTktMDQtMTUvMDU0ODIwNzc3NDUwMTM4OS0yMDE5MDQxNS1hdXhzZW5zb3Jib3hfY28yLTE4NDYxOC50YXIuYnoyIiwKICAidmFsaWQiOiB0cnVlLAogICJpbmNvbWluZ190aW1lc3RhbXAiOiAiMjAxOS0wNC0yMiAwMzo0MzoyNS4wMDEwMDAiLAogICJvdXRnb2luZ190aW1lc3RhbXAiOiAiMjAxOS0wNC0yMiAwMzo0NToxOCIsCiAgInR5cGUiOiAiYXV4IiwKICAiZmlsZW5hbWUiOiAiMDU0ODIwNzc3NDUwMTM4OS0yMDE5MDQxNS1hdXhzZW5zb3Jib3hfY28yLTE4NDYxOC5hdXgiLAogICJwYXlsb2FkIjogWyJyYXctcHJvY2Vzc29yL3Byb2Nlc3NfZGF0ZT0yMDE5LTA1LTIxL2RhdGF0eXBlPWF1eC9wcm9jZXNzX2lkPWQzMWY0NWFjLTdiZmEtMTFlOS05MWM5LTMwOWMyM2U1ODEwOC9yYXctcHJvY2Vzc29yX18wNTQ4MjA3Nzc0NTAyNjY3LTIwMTkwNTA4LWNvbnRyb2xfdW5pdF9pbXUtMjIwMDUwLnRhci5iejJfXzA1NDgyMDc3NzQ1MDI2NjctMjAxOTA1MDgtY29udHJvbF91bml0X2ltdS0yMjAwNTAuYXV4X19kMzFmNDVhYy03YmZhLTExZTktOTFjOS0zMDljMjNlNTgxMDguY3N2Lmd6Il0KfQoKCg==",
            "approximateArrivalTimestamp": 1556068063.985
          },
          "eventSource": "aws:kinesis",
          "eventVersion": "1.0",
          "eventID": "shardId-000000000000:49594210671589627714656895521215685727132014026751475714",
          "eventName": "aws:kinesis:record",
          "invokeIdentityArn": "arn:aws:iam::425695594515:role/lambda-kinesis-role",
          "awsRegion": "us-west-2",
          "eventSourceARN": "arn:aws:kinesis:us-west-2:425695594515:stream/kinesis-RawDataStream-1R0K6AS5TFUIH"
        }
      ]
    }
    video_event = video_event["Records"][0]
    #print(video_event)

    rp = AuxRecordProcessor(name="video_pro", cloud_provider=SYS_CLOUD_PROVIDER)
    result = rp.process(video_event)
    print(result)
