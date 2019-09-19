from helpers import UNDEFINED, cast_to_int
from models.data_lake_record import DataLakeRecord
from models.record_processor import RecordProcessorBase

from plugins.model_fruit_count_detail.schema_model_fruit_count_detail import schema_model_fruit_count_detail


class ModelFruitCountDetailProcessor(RecordProcessorBase):

    @staticmethod
    def types_to_be_processed():
        return ["model-fruit-count-detail"]

    def get_data_lake_record_class(self):
        return ModelFruitCountDetailDatalakeRecord

    def create_message_payload(self, payload):
        self.current_payload = payload
        record = self.get_data_lake_record_class()(record_processor=self)
        yield record.get_content(payload)

    def get_tag_id(self):
        return self.current_payload["row_location"]["tag_id"]

    def get_customer_id(self):
        return self.current_payload["customer_id"]

    def get_farm_id(self):
        return self.current_payload["farm_id"]

    def get_phase_id(self):
        return self.current_payload["phase_id"]

    def get_upload_timestamp(self):
        return cast_to_int(self.current_payload["upload_timestamp"])

    def get_row_number(self):
        #super().get_row_number()
        return cast_to_int(self.current_payload["row_number"])

    def get_side(self):
        #super().get_side()
        return self.current_payload["side"]

    def get_crops(self):
        #super().get_crops()
        return self.current_payload["crops"]

    def get_machine_id(self):
        #super().get_machine_id()
        return self.current_payload["machine_id"]

    def get_hardware_version(self):
        #super().get_hardware_version()
        return UNDEFINED

    def get_firmware_version(self):
        #super().get_firmware_version()
        return UNDEFINED

    tag_id = property(get_tag_id)


class ModelFruitCountDetailDatalakeRecord(DataLakeRecord):

    record_processor: ModelFruitCountDetailProcessor

    def get_capture_timestamp(self):
        return cast_to_int(self.json_line["capture_timestamp"])

    def get_capture_local_datetime(self):
        return self.json_line["capture_local_datetime"]

    def get_distance_cm(self):
        return cast_to_int(self.json_line["distance"])

    def get_row_session_id(self):
        return self.json_line["row_session_id"]

    def get_velocity(self):
        return cast_to_int(self.json_line["row_location"]["velocity"])

    def get_height_cm(self):
        return cast_to_int(self.json_line["row_location"]["height_cm"])

    def get_record_type(self):
        return "model-fruit-count-detail"  # TODO: check if it is inside the file

    def get_direction(self):
        return self.json_line["camera"]

    def get_cartesian_location(self):
        return self.json_line["cartesian_location"]

    def get_json_schema(self):
        return schema_model_fruit_count_detail

    def get_extra_content(self):
        model_metadata = self.json_line.copy()
        attributes = ["tag_id", "camera", "capture_local_datetime", "capture_timestamp", "cartesian_location", "crops",
                      "customer_id", "distance", "farm_id", "machine_id", "phase_id", "row_location", "row_number",
                      "row_session_id", "side", "upload_timestamp", "capture_local_date"]
        for attr in attributes:
            if attr in model_metadata.keys():
                del model_metadata[attr]

        model_metadata["result_utc_local_datetime"] = self.handle_pandas_null_values(
            self.json_line["result_utc_local_datetime"], "")

        return dict(model_metadata=model_metadata)
