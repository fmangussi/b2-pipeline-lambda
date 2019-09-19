from plugins.model_flower_count_detail.model_flower_count_detail import ModelFlowerCountDetailProcessor, \
    ModelFlowerCountDetailDatalakeRecord
from plugins.model_flower_count_summary.schema_model_flower_count_summary import schema_model_flower_count_summary


class ModelFlowerCountSummaryProcessor(ModelFlowerCountDetailProcessor):

    @staticmethod
    def types_to_be_processed():
        return ["model-flower-count-summary"]

    def get_data_lake_record_class(self):
        return ModelFlowerCountSummaryDatalakeRecord

    def get_tag_id(self):
        return self.current_payload["tag_id"]


class ModelFlowerCountSummaryDatalakeRecord(ModelFlowerCountDetailDatalakeRecord):
    record_processor: ModelFlowerCountSummaryProcessor

    def get_record_type(self):
        return "model-flower-count-summary"

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
        return schema_model_flower_count_summary

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


# if __name__ == '__main__':
#     import config
#     import json
#     p = ModelFlowerCountSummaryProcessor('ModelFlowerCountDetailProcessor', config.aws)
#     d = dict(
#           tag_id="tag_id_todo"
#         , color='red'
#         , count='1'
#         , direction='left'
#         , row_session_id='1558549501473'
#         , farm_id='19fa24cc-e72b-4f9c-9e7f-d39a93090d7d'
#         , phase_id='5eb2eb5f-c509-4a5e-b353-66123bbbbcc3'
#         , capture_local_datetime='2019-05-22 11:25:10'
#         , row_number='132'
#         , side='left'
#         , capture_local_date='2019-05-22'
#         , machine_id='89c5d90e129902e4ba83af9d8c74a5e1'
#         , capture_timestamp='1558549510'
#         , customer_id='c14913a7-3fc1-4764-a856-86fdaa82dcd9'
#         , crops=eval('[{"side": "left", "crop": "TOV", "variation": "TOV"}, {"side": "right", "crop": "TOV", "variation": "TOV"}]')
#         , camera='down'
#         , result_utc_local_datetime='2019-06-07T21:49:29+00:00'
#         , upload_timestamp='1559972969'
#     )
#     output = next(p.create_message_payload(d))
#     print(json.dumps(output, indent=2))
#     try:
#         schema_model_flower_count_summary(output)
#         print("VALID")
#     except Exception as error:
#         print("invalid.")
#         print(error)
