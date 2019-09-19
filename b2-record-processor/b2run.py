#!/home/linuxbrew/.linuxbrew/bin/python3
import base64
import importlib.util
import json
import re
import subprocess
import sys


def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


sys.path.append('./lambda/')
handler = module_from_file("handler", "./lambda/handler.py")

from handler import lambda_handler

if (len(sys.argv) >= 2):
    sys.path.append('./lambda/')

    handler = foo = module_from_file("handler", "./lambda/handler.py")
    from handler import lambda_handler

    event = {
        "Records": [
            {
                "kinesis": {
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": "2019-05-24 09:48:36",
                    "sequenceNumber": "49595739723433006697951543208810580767183725856536133634",
                    "data": "eyJ2ZXJzaW9uIjogMS4wLCAic291cmNlIjogInMzOi8vZWlzLWIyLXVwbG9hZC1kZXYvdmlkZW8vODljNWQ5MGUxMjk5MDJlNGJhODNhZjlkOGM3NGE1ZTEvMjAxOS0wNS0xMy8wNTQ4MjA3Nzc0NTAyMTU3LTIwMTkwNTEzLWRvd24tMjAxMjM5Lmpzb24iLCAidmFsaWQiOiB0cnVlLCAiaW5jb21pbmdfdGltZXN0YW1wIjogIjIwMTktMDUtMjQgMDk6NDg6MzUuNjk2MDAwIiwgIm91dGdvaW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA1LTI0IDA5OjQ4OjM2IiwgInBheWxvYWQiOiBbInJhdy1wcm9jZXNzb3IvcHJvY2Vzc19kYXRlPTIwMTktMDUtMjQvZGF0YXR5cGU9dmlkZW8vcHJvY2Vzc19pZD0xOThjNTEzMi03ZTA5LTExZTktYWUwYi1hNjkxZTJlZThhY2QvcmF3LXByb2Nlc3Nvcl9fMDU0ODIwNzc3NDUwMjE1Ny0yMDE5MDUxMy1kb3duLTIwMTIzOS5qc29uX18wNTQ4MjA3Nzc0NTAyMTU3LTIwMTkwNTEzLWRvd24tMjAxMjM5Lmpzb25fXzE5OGM1MTMyLTdlMDktMTFlOS1hZTBiLWE2OTFlMmVlOGFjZC5jc3YuZ3oiXSwgInR5cGUiOiAidmlkZW8iLCAiZmlsZW5hbWUiOiAidmlkZW8vODljNWQ5MGUxMjk5MDJlNGJhODNhZjlkOGM3NGE1ZTEvMjAxOS0wNS0xMy8wNTQ4MjA3Nzc0NTAyMTU3LTIwMTkwNTEzLWRvd24tMjAxMjM5Lmpzb24ifQ==",
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
        ]}

    path = "raw-processor/process_date=2019-05-27/datatype=aux/process_id=fffb8b28-80be-11e9-91c9-309c23e58108/0548207774501389-20190509-control_unit_pm_temperature-165156.tar.bz2__0548207774501389-20190509-control_unit_pm_temperature-165156.aux.csv.gz"
    path = sys.argv[
        1]  # "raw-processor/process_date=2019-05-27/datatype=aux/process_id=fffb8b28-80be-11e9-91c9-309c23e58108/0548207774501389-20190509-control_unit_pm_temperature-165156.tar.bz2__0548207774501389-20190509-control_unit_pm_temperature-165156.aux.csv.gz"
    file_type = re.compile(r'.*/datatype=(\w+)/.*').match(path).groups()[0]

    data = json.loads(base64.b64decode(event["Records"][0]['kinesis']['data']))
    data['type'] = file_type
    data['payload'] = [path]
    event["Records"][0]['kinesis']['data'] = base64.b64encode(json.dumps(data).encode('UTF-8')).decode('UTF-8')
    f = open('event.json', 'w')
    f.write(json.dumps(event, indent=2))
    f.close()
    print("event updated")
# print(json.dumps(event, indent=2))

if subprocess.call(["sam", "build", "--use-container"]) == 0:
    subprocess.call(["sam", "local", "invoke", "InitFunction"
                        , "--event", "event.json"
                        , "--env-vars", "localenv.json"
                        , "--profile", "b2"
                     ])
else:
    event = json.loads(open('event.json', 'r').read())
    lambda_handler(event, '')
