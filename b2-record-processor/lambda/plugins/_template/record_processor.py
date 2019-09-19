from models.data_lake_record import DataLakeRecord
from models.record_processor import RecordProcessorBase
from .schema import json_schema

from abc import abstractmethod


class TemplateProcessor(RecordProcessorBase):

    @abstractmethod
    def types_to_be_processed(self):
        # return ["tomato-count-details"] # TODO: implement this
        return None

    @abstractmethod
    def create_message_payload(self, payload):
        # yield TemplateDatalakeRecord(record_processor=self).get_content(payload) # TODO: implement this
        return None

    @abstractmethod
    def get_tag_id(self):
        # return self.current_payload["tagId"] # TODO: implement this
        return None

    def get_customer_id(self):
        # return self.current_payload["customer_id"]
        return super().get_customer_id()

    def get_farm_id(self):
        # return self.current_payload["farm_id"]
        return super().get_farm_id()

    def get_phase_id(self):
        # return self.current_payload["phase_id"]
        return super().get_phase_id()

    def get_upload_timestamp(self):
        # return self.current_payload["upload_timestamp"]
        return super().get_upload_timestamp()

    def get_row_number(self):
        # return self.current_payload["row_number"]
        return super().get_row_number()

    def get_side(self):
        # return self.current_payload["side"]
        return super().get_side()

    def get_crops(self):
        # return self.current_payload["crops"]
        return super().get_crops()

    def get_machine_id(self):
        # return self.current_payload["machine_id"]
        return super().get_machine_id()

    def get_hardware_version(self):
        # return self.current_payload["hardware_version"]
        return super().get_hardware_version()

    def get_firmware_version(self):
        # return self.current_payload["firmware_version"]
        return super().get_firmware_version()


class TemplateDatalakeRecord(DataLakeRecord):

    record_processor: TemplateProcessor

    @abstractmethod
    def get_capture_timestamp(self):
        # return self.json_line["capture_timestamp"] # TODO: implement this
        return None

    @abstractmethod
    def get_distance_cm(self):
        # return self.json_line["distance_cm"] # TODO: implement this
        return None

    @abstractmethod
    def get_velocity(self):
        # return self.json_line["velocity"] # TODO: implement this
        return None

    @abstractmethod
    def get_height_cm(self):
        # return self.json_line["height_cm"] # TODO: implement this
        return None

    @abstractmethod
    def get_direction(self):
        # return self.json_line["direction"] # TODO: implement this
        return None

    @abstractmethod
    def get_extra_content(self):
        # return self.json_line["extra_content"] # TODO: implement this
        return None

    @abstractmethod
    def get_json_schema(self):
        return json_schema # TODO: implement this

    def get_capture_local_datetime(self):
        # return self.json_line["capture_local_datetime"]
        return super().get_capture_local_datetime()

    def get_row_session_id(self):
        # return self.json_line["row_session_id"]
        return super().get_row_session_id()

    def get_record_type(self):
        # return self.json_line["record_type"]
        return super().get_record_type()

    def get_cartesian_location(self):
        # return self.json_line["cartesian_location"]
        return super().get_cartesian_location()
