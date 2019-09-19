from plugins.model_fruit_count_detail.model_fruit_count_detail import ModelFruitCountDetailProcessor, \
    ModelFruitCountDetailDatalakeRecord
from plugins.model_fruit_count_summary.schema_model_fruit_count_summary import schema_model_fruit_count_summary


class ModelFruitCountSummaryProcessor(ModelFruitCountDetailProcessor):

    @staticmethod
    def types_to_be_processed():
        return ["model-fruit-count-summary"]

    def get_data_lake_record_class(self):
        return ModelFruitCountSummaryDatalakeRecord

    def get_tag_id(self):
        return self.current_payload["tag_id"]


class ModelFruitCountSummaryDatalakeRecord(ModelFruitCountDetailDatalakeRecord):
    record_processor: ModelFruitCountSummaryProcessor

    def get_record_type(self):
        return "model-fruit-count-summary"

    def get_capture_local_datetime(self):
        return self.json_line["capture_local_datetime"]

    def get_distance_cm(self):
        return 0

    def get_velocity(self):
        return 0

    def get_height_cm(self):
        return 0

    def get_cartesian_location(self):
        return dict(x=0, y=0, z=0)

    def get_json_schema(self):
        return schema_model_fruit_count_summary

    def get_extra_content(self):
        model_metadata = self.json_line.copy()
        attributes = ["tag_id", "camera", "capture_local_datetime", "capture_timestamp", "cartesian_location", "crops",
                      "customer_id", "distance", "farm_id", "machine_id", "phase_id", "row_location", "row_number",
                      "row_session_id", "side", "upload_timestamp", "capture_local_date"]
        for attr in attributes:
            if attr in model_metadata.keys():
                del model_metadata[attr]

        return dict(model_metadata=model_metadata)

