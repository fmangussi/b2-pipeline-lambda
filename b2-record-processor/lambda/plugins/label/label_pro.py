from math import ceil
import datamodule.sysconfig_model as sysconfig_model
import plugins.label.label_schemas as label_schemas
from config import(
    SYS_CLOUD_PROVIDER,
    )
from helpers import cast_to_int
from models.data_lake_record import DataLakeRecord
from models.record_processor import RecordProcessorBase

LABEL_CATEGORY = dict( (x,y) for x, y  in list(map(lambda x: (x.label, x.category),  sysconfig_model.SysConfigModel().labels)))


class LabelRecordProcessor(RecordProcessorBase):

    MAX_DISTANCE = 50

    @staticmethod
    def types_to_be_processed():
        return ["label"]

    def get_number_of_messages(self, json_line):
        distance = abs(cast_to_int(json_line["endDistance"]) - cast_to_int(json_line["startDistance"]))
        number_of_messages = abs(ceil(distance / self.MAX_DISTANCE))
        if distance <= self.MAX_DISTANCE:
            number_of_messages = 1
        return range(0, number_of_messages)

    def create_message_payload(self, payload):
        for index in self.get_number_of_messages(json_line=payload):
            json_line = payload.copy()
            json_line["index"] = index
            record = LabelDatalakeRecord(record_processor=self)
            yield record.get_content(json_line)

    def get_tag_id(self):
        return self.current_payload["tagId"]

    tag_id = property(get_tag_id)


class LabelDatalakeRecord(DataLakeRecord):

    MAX_DISTANCE = 50
    _label_meta = None

    @staticmethod
    def get_new_start(idx, end_distance, start_distance,
                      end_time, start_time, max_distance):
        total_distance = abs(end_distance - start_distance)
        total_time = abs(end_time - start_time)

        if total_distance == 0:
            total_distance = start_time


        distance_on_start = max_distance * idx
        #distance_on_end = max_distance * idx + distance


        new_start_time = (distance_on_start / total_distance) * total_time + start_time
        #new_end_time = (distance_on_end / total_distance) * total_time

        return new_start_time

    @staticmethod
    def get_new_distance(idx, end_distance, start_distance, max_distance):
        if end_distance < start_distance:
            label_payload_start_distance = end_distance
            label_payload_end_distance = start_distance
        else:
            label_payload_start_distance = start_distance
            label_payload_end_distance = end_distance

        new_distance = label_payload_start_distance + (max_distance * idx)
        if new_distance > label_payload_end_distance:
            new_distance = label_payload_end_distance

        return new_distance

    def get_capture_timestamp(self):
        # TODO: review cast to int
        return cast_to_int(self.json_line["startTime"])
    # def get_capture_timestamp(self):
    #     return int(self.get_new_start(
    #         idx=self.json_line["index"],
    #         end_distance=int(self.json_line["endDistance"]),
    #         start_distance=int(self.json_line["startDistance"]),
    #         end_time=float(self.json_line["endTime"]),
    #         start_time=float(self.json_line["startTime"]),
    #         max_distance=self.MAX_DISTANCE))

    def get_distance_cm(self):
        return self.get_new_distance(
            idx=self.json_line["index"],
            end_distance=cast_to_int(self.json_line["endDistance"]),
            start_distance=cast_to_int(self.json_line["startDistance"]),
            max_distance=self.MAX_DISTANCE,
        )

    def get_velocity(self):
        def distance():
            return abs(cast_to_int(self.json_line['startDistance']) - cast_to_int(self.json_line['endDistance']))

        def total_time():
            return abs(float(self.json_line['endTime']) - float(self.json_line['startTime']))

        return distance() / total_time()

    def get_height_cm(self):
        return self.record_processor.get_height_cm()

    def get_record_type(self):
        return self.record_processor.get_record_type()

    def get_direction(self):
        return self.json_line["side"]

    def get_json_schema(self):
        return label_schemas.schema
    
    def get_extra_content(self):
        return {
            "label_meta": self.get_label_meta(),
        }

    def get_label_meta(self):
        if not self._label_meta:
            self._label_meta = self.json_line.copy()
            non_meta_keys = ['seq', 'tagId', 'index']
            for key in non_meta_keys:
                if key in self._label_meta:
                    self._label_meta.pop(key)
            self._label_meta['category'] = LABEL_CATEGORY.get(self._label_meta['label'], self._label_meta['label'])
            self._label_meta['source'] = 'machine'
        return self._label_meta


























if __name__ == "__main__":
    label_event = {
      "Records": [
        {
          "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": "2019-05-24 09:48:36",
            "sequenceNumber": "49595739723433006697951543208810580767183725856536133634",
            "data": "eyJ2ZXJzaW9uIjogMS4wLCAic291cmNlIjogInMzOi8vZWlzLWIyLXVwbG9hZC1kZXYvdmlkZW8vODljNWQ5MGUxMjk5MDJlNGJhODNhZjlkOGM3NGE1ZTEvMjAxOS0wNS0xMy8wNTQ4MjA3Nzc0NTAyMTU3LTIwMTkwNTEzLWRvd24tMjAxMjM5Lmpzb24iLCAidmFsaWQiOiB0cnVlLCAiaW5jb21pbmdfdGltZXN0YW1wIjogIjIwMTktMDUtMjQgMDk6NDg6MzUuNjk2MDAwIiwgIm91dGdvaW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA1LTI0IDA5OjQ4OjM2IiwgInBheWxvYWQiOiBbInJhdy1wcm9jZXNzb3IvcHJvY2Vzc19kYXRlPTIwMTktMDYtMDIvZGF0YXR5cGU9bGFiZWwvcHJvY2Vzc19pZD1iNzk5MzFhMi04NTMzLTExZTktOWNjNS1iZWVhMTdmZjUxYmMvYjc5OTM3MzgtODUzMy0xMWU5LTljYzUtYmVlYTE3ZmY1MWJjX18wNTQ4MjA3Nzc0NTA4Mjk5LVRCRC0yMDE5MDUyMi1sZWZ0LTE4MzcyNC50YXIuYnoyLmNzdi5neiJdLCAidHlwZSI6ICJsYWJlbCIsICJmaWxlbmFtZSI6ICJ2aWRlby84OWM1ZDkwZTEyOTkwMmU0YmE4M2FmOWQ4Yzc0YTVlMS8yMDE5LTA1LTEzLzA1NDgyMDc3NzQ1MDIxNTctMjAxOTA1MTMtZG93bi0yMDEyMzkuanNvbiJ9",
            "approximateArrivalTimestamp": 1558691316.08
          },
          "eventSource": "aws:kinesis",
          "eventVersion": "1.0",
          "eventID": "shardId-000000000000:49595739723433006697951543208810580767183725856536133634",
          "eventName": "aws:kinesis:record",
          "invokeIdentityArn": "arn:aws:iam::019997017433:role/lambda-b2-record-processor-InitFunctionRole-Q5SNGFMZ8QXJ",
          "awsRegion": "us-west-2",
          "eventSourceARN": "arn:aws:kinesis:us-west-2:019997017433:stream/eis-b2-raw-stream"
        }
      ]
    }
    label_event = label_event["Records"][0]
    #print(label_event)

    rp = LabelRecordProcessor(name="label_pro", cloud_provider=SYS_CLOUD_PROVIDER)
    result = rp.process(label_event)
    #print(json.dumps(result, indent=4))
