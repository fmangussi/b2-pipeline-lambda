from helpers import UNDEFINED, cast_to_int
from models.data_lake_record import DataLakeRecord
from models.record_processor import RecordProcessorBase

from plugins.model_flower_count_detail.schema_model_flower_count_detail import schema_model_flower_count_detail


class ModelFlowerCountDetailProcessor(RecordProcessorBase):

    @staticmethod
    def types_to_be_processed():
        return ["model-flower-count-detail"]

    def get_data_lake_record_class(self):
        return ModelFlowerCountDetailDatalakeRecord

    def create_message_payload(self, payload):
        self.current_payload = payload
        record = self.get_data_lake_record_class()(record_processor=self)
        yield record.get_content(payload)

    def get_tag_id(self):
        try:
            return self.current_payload["row_location"]["tag_id"]
        except TypeError as error:
            raise ValueError(f'{self.__class__.__name__}.tag_id: {str(error)}. Detail: {str(self.current_payload)}')

    def get_customer_id(self):
        return self.current_payload["customer_id"]

    def get_farm_id(self):
        return self.current_payload["farm_id"]

    def get_phase_id(self):
        return self.current_payload["phase_id"]

    def get_upload_timestamp(self):
        return cast_to_int(self.current_payload["upload_timestamp"])

    def get_row_number(self):
        # super().get_row_number()
        return cast_to_int(self.current_payload["row_number"])

    @staticmethod
    def get_post_number():
        # super().get_post_number()
        return 0  # self.current_payload["post_number"] # TODO: implement this

    def get_side(self):
        # super().get_side()
        return self.current_payload["side"]

    def get_crops(self):
        # super().get_crops()
        return self.current_payload["crops"]

    def get_machine_id(self):
        # super().get_machine_id()
        return self.current_payload["machine_id"]

    def get_hardware_version(self):
        # super().get_hardware_version()
        return UNDEFINED

    def get_firmware_version(self):
        # super().get_firmware_version()
        return UNDEFINED

    tag_id = property(get_tag_id)


class ModelFlowerCountDetailDatalakeRecord(DataLakeRecord):
    record_processor: ModelFlowerCountDetailProcessor

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
        return "model-flower-count-detail"  # TODO: check if it is inside the file

    def get_direction(self):
        return self.json_line["camera"]

    def get_cartesian_location(self):
        return self.json_line["cartesian_location"]

    def get_json_schema(self):
        return schema_model_flower_count_detail

    def get_extra_content(self):
        model_metadata = self.json_line.copy()
        attributes = ["camera", "capture_local_datetime", "capture_timestamp", "cartesian_location", "crops",
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
#     p = ModelFlowerCountDetailProcessor('ModelFlowerCountDetailProcessor', config.aws)
#     d = dict(tomato_id='0'
#         , video_frame='1'
#         , row_session_id='1558549501473'
#         , distance='0'
#         , direction='left'
#         , x='149'
#         , y='120'
#         , w='20'
#         , h='16'
#         , color='green'
#         , color_code='113'
#         , filename='0548207774508299-1558549501473-20190522-down-182509.mkv'
#         , farm_id='19fa24cc-e72b-4f9c-9e7f-d39a93090d7d'
#         , phase_id='5eb2eb5f-c509-4a5e-b353-66123bbbbcc3'
#         , capture_local_datetime='2019-05-22 11:25:10'
#         , row_number='132'
#         , side='left'
#         , capture_local_date='2019-05-22'
#         , machine_id='89c5d90e129902e4ba83af9d8c74a5e1'
#         , capture_timestamp='1558549510'
#         , customer_id='c14913a7-3fc1-4764-a856-86fdaa82dcd9'
#         , row_location=eval('{"tag_id": "0548207774508299", "row_number": 132, "distance_cm": 0, "height_cm": 66, "post_number": 0, "side": "left", "direction": "down", "velocity": 22.0}')
#         , cartesian_location=eval('{"x": 9000, "y": 0, "z": 66}')
#         , crops=eval('[{"side": "left", "crop": "TOV", "variation": "TOV"}, {"side": "right", "crop": "TOV", "variation": "TOV"}]')
#         , camera='down'
#         , result_utc_local_datetime='2019-06-07T21:49:29+00:00'
#         , upload_timestamp='1559972969'
#     )
#     output = next(p.create_message_payload(d))
#     print(json.dumps(output, indent=2))
#     try:
#         schema_model_flower_count_detail(output)
#         print("VALID")
#     except Exception as error:
#         print("invalid.")
#         print(error)
