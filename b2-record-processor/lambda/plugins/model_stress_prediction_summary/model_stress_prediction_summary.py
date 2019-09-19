from helpers import cast_to_int
from plugins.label import label_schemas

from plugins.label.label_pro import LABEL_CATEGORY

from plugins.model_stress_prediction_detail.model_stress_prediction_detail import ModelStressPredictionDetailProcessor, \
    ModelStressPredictionDetailDatalakeRecord


class ModelStressPredictionSummaryProcessor(ModelStressPredictionDetailProcessor):

    @staticmethod
    def types_to_be_processed():
        return ["model-stress-prediction-summary"]

    def send_to_processed_data_stream(self):
        if self.stream_message and self.stream_message.type:
            self.stream_message.type = 'label'
        super().send_to_processed_data_stream()

    def create_message_payload(self, payload):
        return super().create_message_payload(payload)

    def get_data_lake_record_class(self):
        return ModelStressPredictionSummaryDatalakeRecord

    def get_side(self):
        return self.current_payload["side"]


class ModelStressPredictionSummaryDatalakeRecord(ModelStressPredictionDetailDatalakeRecord):

    def __init__(self, record_processor):
        super().__init__(record_processor)
        self._start_time = None

    def get_distance_cm(self):
        return cast_to_int(self.record_processor.get_row_location()["distance_cm"])

    def get_record_type(self):
        return "label"

    def get_json_schema(self):
        return label_schemas.schema

    def get_direction(self):
        return self.json_line["row_side"]

    def get_extra_content(self):
        label_meta = self.json_line.copy()
        attributes = ["tag_id", "camera", "greenhouse_side", "capture_local_datetime", "capture_timestamp",
                      "cartesian_location", "crops", "customer_id", "distance", "farm_id", "machine_id",
                      "phase_id", "row_location", "row_number", "row_session_id", "side",
                      "upload_timestamp", "capture_local_date"]
        for attr in attributes:
            if attr in label_meta.keys():
                del label_meta[attr]

        label_meta["startDistance"] = self.get_distance_cm()
        label_meta["rsid"] = self.get_row_session_id()
        label_meta["startTime"] = self.get_capture_local_datetime()
        label_meta["endTime"] = self.get_capture_local_datetime()
        label_meta["endDistance"] = self.get_distance_cm()
        label_meta["category"] = LABEL_CATEGORY.get(self.json_line["label"], self.json_line["label"])

        return dict(label_meta=label_meta)
