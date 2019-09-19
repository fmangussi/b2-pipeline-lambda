# from abc import ABC, abstractmethod
# from pprint import pprint

# import base64

# import json

# class EventMessage(ABC):

#     _content = None


#     def __init__(self, content):
#         self._content = content

    

#     @abstractmethod
#     def _get_event_data_encoded(self):
#         """data in the base64 format"""

#     @abstractmethod
#     def get_event_data_decoded(self):
#         """message decoded from base64 data"""

#     @abstractmethod
#     def get_event_timestamp_epoch(self):
#         """approximateArrivalTimestamp"""


# class AWSKinesisEventMessage(EventMessage):

#     def _get_event_data_encoded(self):
#         return self._content["kinesis"]["data"]

#     def get_event_data_decoded(self):
#         return json.loads(
#             base64.b64decode(
#                 self._get_event_data_encoded()).decode('utf-8'))

#     def get_event_timestamp_epoch(self):
#         return self._content["kinesis"]["approximateArrivalTimestamp"]


# class RecordProcessMessage:

#     def __init__(self, event_message):
#         """Summary
        
#         Args:
#             event_message (EventMessage): instance of EventMessage class
#         """
#         self._event_data_decoded = event_message.get_event_data_decoded()

#     @property
#     def version(self):
#         return self._event_data_decoded["version"]

#     @property
#     def source(self):
#         return self._event_data_decoded["source"]
    
#     @property
#     def valid(self):
#         return self._event_data_decoded["valid"]
    
#     @property
#     def incoming_timestamp(self):
#         return self._event_data_decoded["incoming_timestamp"]

#     @property
#     def outgoing_timestamp(self):
#         return self._event_data_decoded["outgoing_timestamp"]

#     @property
#     def payload(self):
#         return self._event_data_decoded["payload"]

#     @property
#     def type(self):
#         return self._event_data_decoded["type"]

#     @property
#     def filename(self):
#         return self._event_data_decoded["filename"]

#     def __str__(self):
#         return str(self._event_data_decoded)
    


# if __name__ == "__main__":
#     event = {
#       "kinesis": {
#         "COMMENT": "LABEL DATA",
#         "kinesisSchemaVersion": "1.0",
#         "partitionKey": "2019-04-25 15:45:32",
#         "sequenceNumber": "49594210671678830695451044071707598249733934195629097026",
#         "data": "eyJ2ZXJzaW9uIjogMS4wLCAic291cmNlIjogImxhYmVsLzg5NGY5NDQyOGE1NjJiYTdmNTI0ZjQwYjYyODAyNDcyLzIwMTktMDQtMjMvMDQzODc2MzIzNTg5MDA1Ni0yMDE5MDQyMy1sZWZ0LTE5MTEzNC50YXIuYnoyIiwgInZhbGlkIjogdHJ1ZSwgImluY29taW5nX3RpbWVzdGFtcCI6ICIxOTcwLTAxLTAxIDAwOjAwOjAwIiwgIm91dGdvaW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA0LTI1IDE1OjQ1OjMyIiwgInR5cGUiOiAibGFiZWwiLCAiZmlsZW5hbWUiOiAiMDQzODc2MzIzNTg5MDA1Ni0yMDE5MDQyMy1sZWZ0LTE5MTEzNC5sYWJlbCIsICJwYXlsb2FkIjogeyJzdGFydERpc3RhbmNlIjogMTI0LCAic2VxIjogIjg3MWZjMmYxZTA1OTI5OGEwYWEzZmMwZDZhMzY2YjllIiwgInRhZ0lkIjogIjA0Mzg3NjMyMzU4OTAwNTYiLCAiZW5kRGlzdGFuY2UiOiAyNTAsICJsYWJlbCI6ICJjYXRlcnBpbGxhcnMiLCAicHJlc3N1cmUiOiAzLCAic3RhcnRUaW1lIjogMTU1NjA0NjY4MC42MjAwNzksICJlbmRUaW1lIjogMTU1NjA0NjY5NC41MzE3MiwgInNpZGUiOiAibGVmdCJ9fQ==",
#         "approximateArrivalTimestamp": 1556207132.798
#       },
#       "eventSource": "aws:kinesis",
#       "eventVersion": "1.0",
#       "eventID": "shardId-000000000004:49594210671678830695451044071707598249733934195629097026",
#       "eventName": "aws:kinesis:record",
#       "invokeIdentityArn": "arn:aws:iam::425695594515:role/lambda-kinesis-role",
#       "awsRegion": "us-west-2",
#       "eventSourceARN": "arn:aws:kinesis:us-west-2:425695594515:stream/kinesis-RawDataStream-1R0K6AS5TFUIH"
#     }

#     # { # LABEL
#     #   "startDistance": 124,
#     #   "seq": "871fc2f1e059298a0aa3fc0d6a366b9e",
#     #   "tagId": "0438763235890056",
#     #   "endDistance": 250,
#     #   "label": "caterpillars",
#     #   "pressure": 3,
#     #   "startTime": 1556046680.620079,
#     #   "endTime": 1556046694.53172,
#     #   "side": "left"
#     # }

#     # { # AUX
#     #   "seg": 84341,
#     #   "node": "auxsensorbox",
#     #   "co2": 738.0,
#     #   "location": {
#     #     "distance": 162,
#     #     "tagid": "0548207774501389",
#     #     "velocity": 71,
#     #     "height": 55
#     #   },
#     #   "time": 1555348352.751115,
#     #   "namespace": "/auxsensorbox/co2",
#     #   "id": "co2"
#     # }

#     # { # IMAGE
#     #   "image_name": "0000000182176344-20190422-up_left-204113.jpg",
#     #   "location": {
#     #     "distance": 42,
#     #     "tagid": "0000000182176344",
#     #     "velocity": 14,
#     #     "height": 66
#     #   },
#     #   "id": "image_info",
#     #   "time": 1555965673.868792,
#     #   "image_file_path": "s3://ecoation-b-f-key-value-store/image/0000000182176344-20190422-up_left-204210.jpg"
#     # }    

#     # { # VIDEO
#     #   "id": "frame_info",
#     #   "location": {
#     #     "distance": 69,
#     #     "velocity": 17.0,
#     #     "height": 68,
#     #     "tagid": "0000000182176344"
#     #   },
#     #   "frames": [{"frame_num":11,"distance":59},{"frame_num":12,"distance":63},{"frame_num":13,"distance":63},{"frame_num":15,"distance":66},{"frame_num":16,"distance":66},{"frame_num":17,"distance":66},{"frame_num":18,"distance":69},{"frame_num":19,"distance":69},{"frame_num":20,"distance":69}],
#     #   "time": 1555965676.0,
#     #   "video_file_path": "s3://ecoation-b-f-key-value-store/video/0000000182176344-20190422-down-204113.mkv"
#     # }

#     # [ # WAVE
#     #     {
#     #       "instance": "0",
#     #       "id": "device-id",
#     #       "param": "eis"
#     #     },
#     #     {
#     #       "value": "2019-04-01-155327",
#     #       "id": "samplingTime",
#     #       "param": "utc"
#     #     },
#     #     {
#     #       "value": 1554134007.0,
#     #       "id": "samplingTime",
#     #       "param": "unix"
#     #     },
#     #     {
#     #       "cfgName": "sample_recipe",
#     #       "image": {
#     #         "enable": false
#     #       },
#     #       "spectrumSolid": {
#     #         "intensity": [
#     #           1750,
#     #           1750,
#     #           1750,
#     #           1750,
#     #           1750
#     #         ],
#     #         "enable": true,
#     #         "flashOrder": [
#     #           "Halogen"
#     #         ],
#     #         "sensorSide": 3,
#     #         "integrationTime": 30
#     #       },
#     #       "spectrumPulse": {
#     #         "FLASH_8": "BASELINE",
#     #         "FLASH_9": "BASELINE",
#     #         "flashDutyUp": 2500,
#     #         "enable": true,
#     #         "flashDutyDown": 500,
#     #         "FLASH_1": "Green",
#     #         "FLASH_2": "BASELINE",
#     #         "FLASH_3": "Red",
#     #         "FLASH_4": "BASELINE",
#     #         "FLASH_5": "UV",
#     #         "FLASH_6": "BASELINE",
#     #         "FLASH_7": "Blue",
#     #         "numberOfProbe": 7,
#     #         "intensity": [
#     #           1000,
#     #           1000,
#     #           1000,
#     #           1000,
#     #           1000
#     #         ],
#     #         "numberOfPreReading": 1,
#     #         "sensorSide": 3,
#     #         "numberOfScans": 1,
#     #         "integrationTime": 100
#     #       },
#     #       "id": "cfg"
#     #     },
#     #     {
#     #       "distance": 152,
#     #       "tagid": "40000010106FC4",
#     #       "velocity": 5,
#     #       "id": "location",
#     #       "height": 98
#     #     },
#     #     {
#     #       "wavelength": "wave/40000010106FC4-20190401-0-155327-wave-info-pulse-FLMS0-12479.json",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave-info",
#     #       "method": "pulse"
#     #     },
#     #     {
#     #       "counter": 1,
#     #       "probe": "BASELINE",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-1-pulse-BASELINE-FLMS0-12479.json",
#     #       "method": "pulse",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "counter": 2,
#     #       "probe": "Green",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-2-pulse-Green-FLMS0-12479.json",
#     #       "method": "pulse",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "counter": 3,
#     #       "probe": "BASELINE",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-3-pulse-BASELINE-FLMS0-12479.json",
#     #       "method": "pulse",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "counter": 4,
#     #       "probe": "Red",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-4-pulse-Red-FLMS0-12479.json",
#     #       "method": "pulse",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "counter": 5,
#     #       "probe": "BASELINE",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-5-pulse-BASELINE-FLMS0-12479.json",
#     #       "method": "pulse",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "counter": 6,
#     #       "probe": "UV",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-6-pulse-UV-FLMS0-12479.json",
#     #       "method": "pulse",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "counter": 7,
#     #       "probe": "BASELINE",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-7-pulse-BASELINE-FLMS0-12479.json",
#     #       "method": "pulse",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "counter": 8,
#     #       "probe": "Blue",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-8-pulse-Blue-FLMS0-12479.json",
#     #       "method": "pulse",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "value": [
#     #         [
#     #           0,
#     #           "Green",
#     #           108,
#     #           374,
#     #           281,
#     #           259
#     #         ],
#     #         [
#     #           1,
#     #           "BASELINE",
#     #           0,
#     #           0,
#     #           0,
#     #           0
#     #         ],
#     #         [
#     #           2,
#     #           "Red",
#     #           298,
#     #           382,
#     #           394,
#     #           386
#     #         ],
#     #         [
#     #           3,
#     #           "BASELINE",
#     #           0,
#     #           0,
#     #           0,
#     #           0
#     #         ],
#     #         [
#     #           4,
#     #           "UV",
#     #           389,
#     #           405,
#     #           412,
#     #           409
#     #         ],
#     #         [
#     #           5,
#     #           "BASELINE",
#     #           0,
#     #           0,
#     #           0,
#     #           0
#     #         ],
#     #         [
#     #           6,
#     #           "Blue",
#     #           152,
#     #           391,
#     #           376,
#     #           364
#     #         ]
#     #       ],
#     #       "id": "LEDFaultDetection",
#     #       "param": "average",
#     #       "method": "pulse"
#     #     },
#     #     {
#     #       "wavelength": "wave/40000010106FC4-20190401-0-155327-wave-info-solid-FLMS0-12479.json",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave-info",
#     #       "method": "solid"
#     #     },
#     #     {
#     #       "counter": 9,
#     #       "probe": "BASELINE",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-9-solid-BASELINE-FLMS0-12479.json",
#     #       "method": "solid",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "wavelength": "wave/40000010106FC4-20190401-0-155327-wave-info-solid-FLMS0-12479.json",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave-info",
#     #       "method": "solid"
#     #     },
#     #     {
#     #       "counter": 10,
#     #       "probe": "Halogen",
#     #       "value": "wave/40000010106FC4-20190401-0-155327-wave-10-solid-Halogen-FLMS0-12479.json",
#     #       "method": "solid",
#     #       "serial": "FLMS0-12479",
#     #       "id": "wave"
#     #     },
#     #     {
#     #       "value": [
#     #         [
#     #           1,
#     #           "Halogen",
#     #           380,
#     #           379,
#     #           380,
#     #           379
#     #         ]
#     #       ],
#     #       "id": "LEDFaultDetection",
#     #       "param": "average",
#     #       "method": "solid"
#     #     }
#     #   ]    

#     m = AWSKinesisEventMessage(content=event)
#     r = RecordProcessMessage(event_message=m)
#     pprint(type(r.payload))
#     pprint(str(r))