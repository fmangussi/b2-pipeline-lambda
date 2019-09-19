from helpers import cast_to_int
from models.data_lake_record import DataLakeRecord

import plugins.image.image_schemas as image_schemas

from config import SYS_CLOUD_PROVIDER

from models.record_processor import RecordProcessorBase


class ImageRecordProcessor(RecordProcessorBase):

    @staticmethod
    def types_to_be_processed():
        return ["image"]

    def create_message_payload(self, payload):
        record = ImageDatalakeRecord(record_processor=self)
        yield record.get_content(payload)

    def get_tag_id(self):
        return self.current_payload["location"]["tagid"]

    def get_direction(self):
        # return ospath.basename(self.stream_message.filename).split('-')[2]
        direction = self.get_filename_info()['extra_info']
        if direction == '0':
            direction = 'left'
        elif direction == '1':
            direction = 'right'
        return direction

    def get_camera(self):
        # return ospath.basename(self.stream_message.filename).split('-')[2]
        return self.get_filename_info()['extra_info']

    tag_id = property(get_tag_id)
    camera = property(get_camera)
    direction = property(get_direction)


class ImageDatalakeRecord(DataLakeRecord):

    def get_json_schema(self):
        return image_schemas.image_json_schema["image_data_payload"]

    def get_capture_timestamp(self):
        return cast_to_int(self.json_line["time"])

    def get_distance_cm(self):
        # print("self.json_line", self.json_line)
        return self.json_line["location"]["distance"]

    def get_velocity(self):
        return self.json_line['location']['velocity']

    def get_height_cm(self):
        return self.json_line['location']['height']

    def get_image_file_path(self):
        return self.json_line["image_file_path"]

    def get_direction(self):
        return self.record_processor.get_direction()

    def get_camera_metadata(self):
        return {}

    def get_extra_content(self):
        return {
            "image_file_path": self.get_image_file_path(),
            "camera": self.record_processor.get_camera(),
            "camera_metadata": self.get_camera_metadata()
        }


if __name__ == "__main__":
    video_event = {
        "Records": [
            {
                "kinesis": {
                    "COMMENT": "VIDEO BEGIN",
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "2019-04-24 01:07:43",
                    "sequenceNumber": "49594210671589627714656895521215685727132014026751475714",
                    "data": "ewogICJ2ZXJzaW9uIjogMS4wLAogICJzb3VyY2UiOiAiaW1hZ2UvODk0Zjk0NDI4YTU2MmJhN2Y1MjRmNDBiNjI4MDI0NzIvMjAxOS0wNC0yMi8wMDAwMDAwMTgyMTc2MzQ0LTIwMTkwNDIyLXVwX2xlZnQtMjA0MjEwLmpzb24iLAogICJ2YWxpZCI6IHRydWUsCiAgImluY29taW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA0LTI1IDAwOjEwOjQ2LjM1MjAwMCIsCiAgIm91dGdvaW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA0LTI1IDAwOjM5OjE1IiwKICAidHlwZSI6ICJpbWFnZSIsCiAgImZpbGVuYW1lIjogIjAwMDAwMDAxODIxNzYzNDQtMjAxOTA0MjItdXBfbGVmdC0yMDQyMTAuanNvbiIsCiAgInBheWxvYWQiOiBbInJhdy1wcm9jZXNzb3IvcHJvY2Vzc19kYXRlPTIwMTktMDUtMjEvZGF0YXR5cGU9aW1hZ2UvcHJvY2Vzc19pZD02Y2JhYjQyYy03YzIyLTExZTktOTFjOS0zMDljMjNlNTgxMDgvcmF3LXByb2Nlc3Nvcl9fMDAwMDAwMDE4MjE3NjM0NC0yMDE5MDQyMi11cF9sZWZ0LTIwNDIxMC5qc29uX18wMDAwMDAwMTgyMTc2MzQ0LTIwMTkwNDIyLXVwX2xlZnQtMjA0MjEwLmpzb25fXzZjYmFiNDJjLTdjMjItMTFlOS05MWM5LTMwOWMyM2U1ODEwOC5jc3YuZ3oiXQp9",
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
    # print(video_event)

    rp = ImageRecordProcessor(name="video_pro", cloud_provider=SYS_CLOUD_PROVIDER)
    result = rp.process(video_event)
    # print(json.dumps(result, indent=4))
