import base64
import json
import os
import re
import subprocess
from contextlib import contextmanager
from pathlib import Path

import s3fs

s3_input_file_key_re =  re.compile(r'^s3://(?P<s3_bucket_name>[^/]+)/(?P<s3_file_key>(?P<base_folder>.*/datatype=(?P<file_type>\w+))/(?P<filename>[^/]+))')
fs = s3fs.S3FileSystem()


def extract_s3_file_info(s3_file_path):
    m = s3_input_file_key_re.match(s3_file_path)
    if not m:
        raise ValueError("The S3 file path didn't match with the actual file path structure.")

    return m.groupdict()


def format_local_file_name_path(base_folder, file_name):
    return os.path.join(os.getcwd(), '..', 'tests', 'stage_files', base_folder, file_name)


def upload_file(stage_file_path):
    s3_file_info = extract_s3_file_info(stage_file_path)
    local_file_path = format_local_file_name_path(base_folder=(s3_file_info["file_type"] or s3_file_info["base_folder"]),
                                                  file_name=s3_file_info["filename"])
    s3_file_path = f's3://{os.getenv("STAGE_BUCKET_NAME", "eis-b2-staging-data-dev")}/{s3_file_info["s3_file_key"]}'
    print("local_file_path", local_file_path)
    try:
        fs.ls(s3_file_path)
    except FileNotFoundError:
        if not os.path.exists(local_file_path):
            raise FileNotFoundError("The local stage file was not found. Details: %s" % local_file_path)
        else:
            print("Uploading the local stage file")
            fs.put(local_file_path, s3_file_path)

    # download file
    if not os.path.exists(local_file_path):
        base_folder = os.path.dirname(local_file_path)
        if not os.path.isdir(base_folder):
            os.mkdir(base_folder)
        fs.get(s3_file_path, local_file_path)


def create_kinesis_message(payload: dict):
    base_message = eval(
        "{'Records': [{'kinesis': {'kinesisSchemaVersion': '1.0', 'partitionKey': '2019-06-07 22:55:46', 'sequenceNumber': '49595739723433006697952404661766786651692610996123205634', 'data': 'eyJ2ZXJzaW9uIjogMS4wLCAic291cmNlIjogIm1vZGVsLXN0cmVzcy1wcmVkaWN0aW9uIiwgInZhbGlkIjogdHJ1ZSwgImluY29taW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA2LTA3VDIyOjU1OjQ2KzAwOjAwIiwgIm91dGdvaW5nX3RpbWVzdGFtcCI6ICIyMDE5LTA2LTA3VDIyOjU1OjQ2KzAwOjAwIiwgInBheWxvYWQiOiAiczM6Ly9laXMtYjItc3RhZ2luZy1kYXRhLWRldi9tb2RlbC1zdHJlc3MtcHJlZGljdGlvbi81ZWIyZWI1Zi1jNTA5LTRhNWUtYjM1My02NjEyM2JiYmJjYzMvMTM2LzE1NTg1NTk0NTY5MDgvOGZkYzMzYzktYTA4Ni00NGZhLThlZmItNWY1ZmNkNzcyNjMwX21vZGVsLXN0cmVzcy1wcmVkaWN0aW9uLXN1bW1hcnkuY3N2IiwgInR5cGUiOiAibW9kZWwtc3RyZXNzLXByZWRpY3Rpb24tc3VtbWFyeSIsICJmaWxlbmFtZSI6ICIxNTU4NTU5NDU2OTA4LzhmZGMzM2M5LWEwODYtNDRmYS04ZWZiLTVmNWZjZDc3MjYzMF9tb2RlbC1zdHJlc3MtcHJlZGljdGlvbi1zdW1tYXJ5LmNzdiJ9', 'approximateArrivalTimestamp': 1559948146.812}, 'eventSource': 'aws:kinesis', 'eventVersion': '1.0', 'eventID': 'shardId-000000000000:49595739723433006697952404661766786651692610996123205634', 'eventName': 'aws:kinesis:record', 'invokeIdentityArn': 'arn:aws:iam::019997017433:role/lambda-b2-record-processor-InitFunctionRole-Q5SNGFMZ8QXJ', 'awsRegion': 'us-west-2', 'eventSourceARN': 'arn:aws:kinesis:us-west-2:019997017433:stream/eis-b2-raw-stream'}]}")
    data = base64.b64encode(json.dumps(payload).encode(encoding='UTF-8')).decode('UTF-8')
    base_message["Records"][0]["kinesis"]["data"] = data
    return base_message


def model_stress_prediction_summary_event():
    payload = {'version': 1.0,
               'source': 'model-stress-prediction',
               'valid': True,
               'incoming_timestamp': '2019-06-07T22:55:46+00:00',
               'outgoing_timestamp': '2019-06-07T22:55:46+00:00',
               'payload': 's3://eis-b2-staging-data-dev/model-stress-prediction/unittest-5eb2eb5f-c509-4a5e-b353-66123bbbbcc3/136/1558559456908/8fdc33c9-a086-44fa-8efb-5f5fcd772630_model-stress-prediction-summary.csv',
               'type': 'model-stress-prediction-summary',
               'filename': '1558559456908/8fdc33c9-a086-44fa-8efb-5f5fcd772630_model-stress-prediction-summary.csv'}
    upload_file(payload["payload"])
    return create_kinesis_message(payload)


def model_stress_prediction_detail_event():
    payload = {'version': 1.0,
               'source': 'model-stress-prediction',
               'valid': True,
               'incoming_timestamp': '2019-06-07T22:55:46+00:00',
               'outgoing_timestamp': '2019-06-07T22:55:46+00:00',
               'payload': 's3://eis-b2-staging-data-dev/model-stress-prediction/unittest-5eb2eb5f-c509-4a5e-b353-66123bbbbcc3/136/1558559456908/8926ea6c-b22c-4492-8618-88500faa6447_model-stress-prediction-detail.csv',
               'type': 'model-stress-prediction-detail',
               'filename': '1558559456908/8926ea6c-b22c-4492-8618-88500faa6447_model-stress-prediction-detail.csv'}
    upload_file(payload["payload"])
    return create_kinesis_message(payload)


def model_fruit_count_detail_event():
    payload = {'version': 1.0,
               'source': 'model-fruit-count',
               'valid': True,
               'incoming_timestamp': '2019-06-07T23:17:05+00:00',
               'outgoing_timestamp': '2019-06-07T23:17:05+00:00',
               'payload': 's3://eis-b2-staging-data-dev/model-fruit-count/unittest-5eb2eb5f-c509-4a5e-b353-66123bbbbcc3/132/1558549501473/f51c31c4-e20e-46eb-8b25-2a29874b2a08_model-fruit-count-detail.csv',
               'type': 'model-fruit-count-detail',
               'filename': 'f51c31c4-e20e-46eb-8b25-2a29874b2a08_model-fruit-count-detail.csv'}
    upload_file(payload["payload"])
    return create_kinesis_message(payload)


def model_fruit_count_summary_event():
    payload = {'version': 1.0,
               'source': 'model-fruit-count',
               'valid': True,
               'incoming_timestamp': '2019-06-07T23:17:05+00:00',
               'outgoing_timestamp': '2019-06-07T23:17:05+00:00',
               'payload': 's3://eis-b2-staging-data-dev/model-fruit-count/unittest-5eb2eb5f-c509-4a5e-b353-66123bbbbcc3/132/1558549501473/cee37a99-13df-4e1c-ade2-c7d9f2018262_model-fruit-count-summary.csv',
               'type': 'model-fruit-count-summary',
               'filename': 'cee37a99-13df-4e1c-ade2-c7d9f2018262_model-fruit-count-summary.csv'}
    upload_file(payload["payload"])
    return create_kinesis_message(payload)


def aux_event():
    payload = {'version': 1.0,
               'source': 's3://eis-b2-upload-dev/aux/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/0548207774499088-1558560182120-20190522-control_unit_imu-213249.tar.bz2',
               'valid': True,
               'incoming_timestamp': '2019-06-08 23:22:25.115000',
               'outgoing_timestamp': '2019-06-08 23:22:27',
               'payload': [
                   's3://eis-b2-staging-data-dev/raw-processor/process_date=2019-06-09/datatype=aux/0548207774491915-1558550387142-20190522-auxsensorbox_co2-184951.tar.bz2__d949b9f6-8ad4-11e9-84ab-6aa4804b18f2.csv.gz'],
               'type': 'aux',
               'filename': 'aux/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/0548207774491915-1558550387142-20190522-auxsensorbox_co2-184951.tar.bz2'}
    upload_file(payload["payload"][0])
    return create_kinesis_message(payload)


def invalid_aux():
    payload = {'version': 1.0,
               'source': 's3://eis-b2-upload-dev/aux/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/TBD-TBD-20190522-saber_sensor_onboard_temperature-215831.tar.bz2',
               'valid': True, 'incoming_timestamp': '2019-06-10 04:26:58.673000',
               'outgoing_timestamp': '2019-06-10 04:27:03',
               'payload': ['s3://eis-b2-staging-data-dev/raw-processor/process_date=2019-06-10/datatype=aux/TBD-TBD-20190522-saber_sensor_onboard_temperature-215831.tar.bz2__ff38be6a-8b37-11e9-8a07-da7573c56728.csv.gz'],
               'type': 'aux',
               'filename': 'aux/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/TBD-TBD-20190522-saber_sensor_onboard_temperature-215831.tar.bz2'}
    upload_file(payload["payload"][0])
    return create_kinesis_message(payload)


def wave_event():
    payload = {'version': 1.0,
               'source': 's3://eis-b2-upload-dev/wave/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/0548207774501389-1558559425861-20190522-0-000000.tar.bz2',
               'valid': True,
               'incoming_timestamp': '2019-06-08 23:22:25.115000',
               'outgoing_timestamp': '2019-06-08 23:22:27',
               'payload': [
                   's3://eis-b2-staging-data-dev/raw-processor/process_date=2019-06-10/datatype=wave/0548207774491915-1558550387142-20190522-0-000000.tar.bz2__0548207774491915-1558550387142-20190522-0-184242.wave__facd5cd0-8bac-11e9-8e1b-d6e4391e65b9.csv.gz'],
               'type': 'wave',
               'filename': 'wave/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/0548207774501389-1558559425861-20190522-0-000000.tar.bz2'}
    upload_file(payload["payload"][0])
    return create_kinesis_message(payload)


@contextmanager
def does_not_raise():
    yield


def stage_files_event_generator(only_names=False, file_type_filter=None):
    stage_files_folder = re.sub(r'^(.*/b2-record-processor)/.*$', r'\1', os.getcwd())
    stage_files_folder = Path(stage_files_folder, 'tests', 'stage_files')
    s3_stage_files_folder = f's3://eis-b2-staging-data-dev/unittest-record-processor/'
    if not only_names:
        subprocess.call(['aws', 's3', 'sync', stage_files_folder, s3_stage_files_folder])
    stage_file_pattern = re.compile(r'^.*/stage_files/.*datatype=(?P<file_key>(?P<file_type>[^/]+).*/[^/]+$)')
    stage_files_list = [item.as_posix() for item in stage_files_folder.glob('**/*.*') if item.as_posix().find('/.')<0]
    if len(stage_files_list) == 0:
        raise AttributeError('List of files is empty. Details: %s' % stage_files_folder.as_posix())
    for filename in stage_files_list:
        try:
            file_type = stage_file_pattern.match(filename).groupdict()['file_type']
            file_key = stage_file_pattern.match(filename).groupdict()['file_key']
            s3_filename = f'{s3_stage_files_folder}datatype={file_key}'
            payload = {'version': 1.0,
                       'source': file_key,
                       'valid': True,
                       'incoming_timestamp': '2019-06-08 23:22:25.115000',
                       'outgoing_timestamp': '2019-06-08 23:22:27',
                       'payload': [
                           s3_filename],
                       'type': file_type,
                       'filename': f'{file_key}'}
            # event, exception_expected, result_expected
            if (not only_names) and ((file_type_filter is None) or (file_type_filter == file_type)):
                yield create_kinesis_message(payload), does_not_raise(), 0
            else:
                if (file_type_filter is None) or (file_type_filter == file_type):
                    yield f'{file_key}'
        except:
            print(f'Ignoring the file {filename}')


if __name__ == '__main__':
    for event in stage_files_event_generator(only_names=True):
        print(event)