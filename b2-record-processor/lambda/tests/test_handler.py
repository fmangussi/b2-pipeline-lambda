import base64
import json
import re
import os

from handler import *


def call_event(filename):
    event = {"Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "0548207774485311-1565891440234-20190815-control_unit_imu-000000.tar.bz2",
                "sequenceNumber": "49598867095732034762887633397069201613841321240549130242",
                "data": "eyJ2ZXJzaW9uIjogMS4wLCAic291cmNlIjogInMzOi8vZWlzLWIyLXVwbG9hZC10ZXN0L2F1eC85Mzg0M2VkMjU4ZDFjODQ2OWE4MGM2ZDM2NzJiMDMzZi8yMDE5LTA4LTE1LzA1NDgyMDc3NzQ0ODUzMTEtMTU2NTg5MTQ0MDIzNC0yMDE5MDgxNS1jb250cm9sX3VuaXRfaW11LTAwMDAwMC50YXIuYnoyIiwgInZhbGlkIjogdHJ1ZSwgImluY29taW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA5LTA0IDIyOjUwOjE2LjcwMzAwMCIsICJvdXRnb2luZ190aW1lc3RhbXAiOiAiMjAxOS0wOS0wNCAyMjo1MDoyMSIsICJwYXlsb2FkIjogWyJzMzovL2Vpcy1iMi1zdGFnaW5nLWRhdGEtdGVzdC9yYXctcHJvY2Vzc29yL3Byb2Nlc3NfZGF0ZT0yMDE5LTA5LTA0L2RhdGF0eXBlPWF1eC8wNTQ4MjA3Nzc0NDg1MzExLTE1NjU4OTE0NDAyMzQtMjAxOTA4MTUtY29udHJvbF91bml0X2ltdS0wMDAwMDAudGFyLmJ6Ml9fNWY5ODIyZjYtY2Y2Ni0xMWU5LWJiNzktZjY1ZjFiOTY1NDg3LmNzdi5neiJdLCAidHlwZSI6ICJhdXgiLCAiZmlsZW5hbWUiOiAiYXV4LzkzODQzZWQyNThkMWM4NDY5YTgwYzZkMzY3MmIwMzNmLzIwMTktMDgtMTUvMDU0ODIwNzc3NDQ4NTMxMS0xNTY1ODkxNDQwMjM0LTIwMTkwODE1LWNvbnRyb2xfdW5pdF9pbXUtMDAwMDAwLnRhci5iejIifQ==",
                "approximateArrivalTimestamp": 1567637421.178
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49598867095732034762887633397069201613841321240549130242",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::019997017433:role/lambda-b2-record-processor-InitFunctionRole-8R3H63E1WGZV",
            "awsRegion": "us-west-2",
            "eventSourceARN": "arn:aws:kinesis:us-west-2:019997017433:stream/eis-b2-raw-stream"
        }]}
    file_type = re.compile(r'^([^/]+)').match(filename).groups()[0]
    if file_type in ['model-flower-count', 'model-fruit-count', 'model-stress-prediction']:
        file_type = re.compile(r'^.*(model-.*)\.csv').match(filename).groups()[0]
    else:
        file_type = re.compile(r'.*/datatype=([^/]+)/').match(filename).groups()[0]
    data = json.loads(base64.b64decode(event["Records"][0]['kinesis']['data']))
    data['type'] = file_type
    data['payload'] = [filename]
    data['source'] = f'{file_type}/{os.path.basename(filename.split("__")[0])}'
    event = {"Records": [{"kinesis": {"kinesisSchemaVersion": "1.0", "partitionKey": "2019-08-27 21:54:01",
                                      "sequenceNumber": "49598828870827516788693986142286294871771267982323351554",
                                      "data": "eyJ2ZXJzaW9uIjogMS4wLCAic291cmNlIjogIm1vZGVsLWZydWl0LWNvdW50IiwgInZhbGlkIjogdHJ1ZSwgImluY29taW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA4LTI3VDIxOjU0OjAxKzAwOjAwIiwgIm91dGdvaW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA4LTI3VDIxOjU0OjAxKzAwOjAwIiwgInBheWxvYWQiOiAiczM6Ly9laXMtYjItc3RhZ2luZy1kYXRhLWRldi9tb2RlbC1mcnVpdC1jb3VudC81ZWIyZWI1Zi1jNTA5LTRhNWUtYjM1My02NjEyM2JiYmJjYzMvMTM1LzE1NjE3NDIzNzkxMDYvZWFkYWJlOTkyOTljOWY3ODk0MTdkMzllZWI5ZmRmOTZfX2U1ZGQ2ZjYzLWI1YmMtNGM3MC05NTZmLTYzNTUwYTg5NmQyMV9tb2RlbC1mcnVpdC1jb3VudC1kZXRhaWwuY3N2IiwgInR5cGUiOiAibW9kZWwtZnJ1aXQtY291bnQtZGV0YWlsIiwgImZpbGVuYW1lIjogImVhZGFiZTk5Mjk5YzlmNzg5NDE3ZDM5ZWViOWZkZjk2X19lNWRkNmY2My1iNWJjLTRjNzAtOTU2Zi02MzU1MGE4OTZkMjFfbW9kZWwtZnJ1aXQtY291bnQtZGV0YWlsLmNzdiJ9",
                                      "approximateArrivalTimestamp": 1566942841.04}, "eventSource": "aws:kinesis",
                          "eventVersion": "1.0",
                          "eventID": "shardId-000000000000:49598828870827516788693986142286294871771267982323351554",
                          "eventName": "aws:kinesis:record",
                          "invokeIdentityArn": "arn:aws:iam::431300170391:role/lambda-b2-record-processor-InitFunctionRole-N3YK6NYE0MFZ",
                          "awsRegion": "us-west-2",
                          "eventSourceARN": "arn:aws:kinesis:us-west-2:431300170391:stream/eis-b2-raw-stream"}]}
    event["Records"][0]['kinesis']['data'] = base64.b64encode(json.dumps(data).encode('UTF-8')).decode('UTF-8')
    lambda_handler(event, context='')


def test_each_type_file():
    flower_count_detail = 'model-flower-count/5eb2eb5f-c509-4a5e-b353-66123bbbbcc3/126/1564518140189/d887f01f360158b18a80a89e767c2038__199f2740-d36f-4dfa-a35d-42d7cdd1bad6_model-flower-count-detail.csv'
    flower_count_summary = 'model-flower-count/5eb2eb5f-c509-4a5e-b353-66123bbbbcc3/126/1564518140189/d887f01f360158b18a80a89e767c2038__43c63ab7-f239-4a3f-98ae-fe1a9f36e158_model-flower-count-summary.csv'
    fruit_count_detail = 'model-fruit-count/5eb2eb5f-c509-4a5e-b353-66123bbbbcc3/115/1565809111026/d3621347062c98e0ebe3087ecfec0518__731aae60-343c-4603-8646-4e80d3da400a_model-fruit-count-detail.csv'
    fruit_count_summary = 'model-fruit-count/5eb2eb5f-c509-4a5e-b353-66123bbbbcc3/115/1565809111026/d3621347062c98e0ebe3087ecfec0518__a4cbfa18-7148-44ac-a377-b796a5dea232_model-fruit-count-summary.csv'
    stress_prediction_detail = 'model-stress-prediction/5eb2eb5f-c509-4a5e-b353-66123bbbbcc3/93/1565891440234/a3491e4ec55570dca7fe4509d0fa15e9__95633242-d1d0-49b8-95d9-8d7c91cc326f_model-stress-prediction-detail.csv'
    aux = 'raw-processor/process_date=2019-08-23/datatype=aux/0548207774527008-1565897431684-20190815-control_unit_imu-000000.tar.bz2__6053c6ba-c56f-11e9-b828-fa4e61c71a1d.csv.gz'
    image = 'raw-processor/process_date=2019-08-23/datatype=image/0548207774527008-1565897431684-20190815-down-000000.tar.bz2__7eb77304-c56f-11e9-8074-ee87a34ce157.csv.gz'
    label = 'raw-processor/process_date=2019-08-23/datatype=label/0548207774485311-1565891440234-20190815-left-000000.tar.bz2__82e3edc2-c56f-11e9-b955-02e10bfe1db9.csv.gz'
    wave = 'raw-processor/process_date=2019-08-23/datatype=wave/0548207774485311-1565891440234-20190815-0-000000.tar.bz2__0548207774485311-1565891440234-20190815-0-175050.wave__0d2c52fc-c580-11e9-8bd6-fe8eff07b37e.csv.gz'
    video = 'raw-processor/process_date=2019-08-23/datatype=video/0548207774485311-1565891440234-20190815-up_right-000000.tar.bz2__332ee766-c5f9-11e9-a38f-5647e878ae84.csv.gz'

    call_event(aux)
    call_event(image)
    call_event(label)
    call_event(wave)
    call_event(video)
    call_event(flower_count_detail)
    call_event(flower_count_summary)
    call_event(fruit_count_detail)
    call_event(fruit_count_summary)
    call_event(stress_prediction_detail)


if __name__ == '__main__':
    test_each_type_file()
