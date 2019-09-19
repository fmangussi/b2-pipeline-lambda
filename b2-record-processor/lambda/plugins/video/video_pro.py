import plugins.video.video_schemas as video_schemas

from config import SYS_CLOUD_PROVIDER
from helpers import cast_to_int
from models.data_lake_record import DataLakeRecord
from models.record_processor import RecordProcessorBase


class VideoRecordProcessor(RecordProcessorBase):

    def __init__(self, name, cloud_provider):
        super().__init__(name, cloud_provider)

    @staticmethod
    def types_to_be_processed():
        return ["video"]

    def create_message_payload(self, payload):
        for frame in payload["frames"]:
            json_line = payload.copy()
            json_line["frame"] = frame
            record = VideoDatalakeRecord(record_processor=self)
            yield record.get_content(json_line)

    def get_tag_id(self):
        return self.current_payload["location"]["tagid"]

    def get_namespace(self):
        return self.get_filename_info()['extra_info']

    def get_direction(self):
        return self.get_namespace()

    def get_camera(self):
        return self.get_namespace()

    tag_id = property(get_tag_id)
    direction = property(get_direction)    
    camera = property(get_camera)

class VideoDatalakeRecord(DataLakeRecord):

    def get_json_schema(self):
        return video_schemas.video_json_schema["video_data_payload"]

    def get_capture_timestamp(self):
        # TODO: review cast to int
        return cast_to_int(self.json_line["time"])

    def get_distance_cm(self):
        #print("self.json_line", self.json_line)
        try:
            return self.json_line["frame"]["distance"]
        except Exception as error:
            print(f"Details: <{str(self.json_line)}>")
            raise ValueError(f"Invalid distance.")


    def get_velocity(self):
        return self.json_line['location']['velocity']

    def get_height_cm(self):
        return self.json_line['location']['height']

    def get_video_file_path(self):
        return self.json_line["video_file_path"]        

    def get_video_frame(self):
        return self.json_line["frame"]["frame_num"]

    def get_direction(self):
        return self.record_processor.get_direction()

    def get_camera_metadata(self):
        return {
            'distance': self.get_distance_cm(),
            'id': self.json_line["id"]
            }

    def get_extra_content(self):
        return {
            "video_file_path": self.get_video_file_path(),
            "video_frame": self.get_video_frame(),
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
            "data": "ewogICJ2ZXJzaW9uIjogMS4wLAogICJzb3VyY2UiOiAidmlkZW8vODk0Zjk0NDI4YTU2MmJhN2Y1MjRmNDBiNjI4MDI0NzIvMjAxOS0wNC0yMi8wMDAwMDAwMTgyMTc2MzQ0LTIwMTkwNDIyLWRvd24tMjA0MTEzLmpzb24iLAogICJ2YWxpZCI6IHRydWUsCiAgImluY29taW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA0LTI0IDAxOjAxOjI2IiwKICAib3V0Z29pbmdfdGltZXN0YW1wIjogIjIwMTktMDQtMjQgMDE6MDc6NDMiLAogICJ0eXBlIjogInZpZGVvIiwKICAiZmlsZW5hbWUiOiAiMDAwMDAwMDE4MjE3NjM0NC0yMDE5MDQyMi1kb3duLTIwNDExMy5qc29uIiwKICAicGF5bG9hZCI6IFsicmF3LXByb2Nlc3Nvci9wcm9jZXNzX2RhdGU9MjAxOS0wNS0yMS9kYXRhdHlwZT12aWRlby9wcm9jZXNzX2lkPTAzM2RiNDU2LTdjMjAtMTFlOS05MWM5LTMwOWMyM2U1ODEwOC9yYXctcHJvY2Vzc29yX18wMDAwMDAwMTgyMTc2MzQ0LTIwMTkwNDIyLWRvd24tMjA0MTEzLmpzb25fXzAwMDAwMDAxODIxNzYzNDQtMjAxOTA0MjItZG93bi0yMDQxMTMuanNvbl9fMDMzZGI0NTYtN2MyMC0xMWU5LTkxYzktMzA5YzIzZTU4MTA4LmNzdi5neiJdCn0=",
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

    rp = VideoRecordProcessor(name="video_pro", cloud_provider=SYS_CLOUD_PROVIDER)
    result = rp.process(video_event)
    #print(json.dumps(result, indent=4))
