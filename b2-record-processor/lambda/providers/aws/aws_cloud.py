#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Created on Fri Apr 12 14:38:46 2019
# @author: mandeep
# Refactored by Farzad Khandan (farzadkhandan@ecoation.com)
#
import csv
import io
import json
import os
import re
import traceback
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
import botocore
import s3fs

from base.cloud_provider import CloudProviderBase
from helpers import (
    get_logger)
from models.event_message import AWSKinesisEventMessage

logger = get_logger(__name__)

CSV_DELIMITER = ';'
CSV_ESCAPECHAR = "\\"
CSV_QUOTECHAR = '"'
CSV_QUOTING = csv.QUOTE_NONNUMERIC
CSV_STRICT = True


class AwsCloudProvider(CloudProviderBase):
    _KINESIS_INVALID_DATASTREAM = os.getenv("INVALID_DATASTREAM_NAME", "eis-b2-invalid-stream")
    _KINESIS_PROCESSED_DATA_STREAM = os.getenv("PROCESSED_DATA_STREAM_NAME", "eis-b2-processed-stream")
    _KINESIS_SAVED_DATA_STREAM = os.getenv("SAVED_DATA_STREAM_NAME", "eis-b2-saved-stream")
    _S3_STAGE_BUCKET_NAME = os.getenv("STAGE_BUCKET_NAME", 'eis-b2-staging-data-test')
    S3_FILE_PATH_RE = re.compile(r'^s3://(?P<bucket_name>[^/]+)/(?P<file_key>.*$)')
    s3fs = s3fs.S3FileSystem()

    def __init__(self):
        super().__init__()
        self.session = boto3.Session()
        self.s3 = self.session.resource('s3')
        self.kinesis_client = self.session.client('kinesis')

    def download_object(self, obj_address, obj_name, local_file_location):
        try:
            self.s3.Bucket(obj_address).download_file(obj_name, local_file_location)
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.info("The object does not exist.")

    def send_to_stream(self, stream_name, stream_payload):
        put_response = self.kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(stream_payload),
            PartitionKey=str(datetime.utcnow().replace(microsecond=0)))
        logger.debug(put_response)

    def put_object(self, obj_address, obj_name, obj_content):
        s3_object = self.s3.Object(obj_address, obj_name)
        s3_object.put(Body=json.dumps(obj_content))

    def extract_bucket_name(self, obj_path, default_bucket=None):
        s3_match = self.S3_FILE_PATH_RE.match(obj_path)
        return s3_match.groupdict()["bucket_name"] if s3_match else default_bucket

    def extract_file_key(self, obj_path):
        s3_match = self.S3_FILE_PATH_RE.match(obj_path)
        return s3_match.groupdict()["file_key"] if s3_match else obj_path

    def get_stage_object(self, obj_name):
        return self.get_object(
            obj_address=self.extract_bucket_name(obj_path=obj_name, default_bucket=self._S3_STAGE_BUCKET_NAME),
            obj_name=self.extract_file_key(obj_path=obj_name))

    def format_stage_filename(self, filepath):
        return f's3://{self._S3_STAGE_BUCKET_NAME}/{self.extract_file_key(obj_path=filepath)}'

    @staticmethod
    def upload_stage_content(data_frame, destination_path):
        data_frame.to_csv(
            path_or_buf=destination_path,
            sep=CSV_DELIMITER,
            escapechar=CSV_ESCAPECHAR,
            quotechar=CSV_QUOTECHAR,
            quoting=CSV_QUOTING,
            index=False
        )
        # with self.s3fs.open(destination_path, 'wb') as s3f:
        #     with gzip.GzipFile(fileobj=s3f) as gz_file:
        #         output = data_frame.to_csv(index=False,
        #                                    sep=CSV_DELIMITER,
        #                                    escapechar=CSV_ESCAPECHAR,
        #                                    quotechar=CSV_QUOTECHAR,
        #                                    quoting=CSV_QUOTING
        #                                    )
        #         gz_file.write(output.encode('UTF-8'))

    def get_object(self, obj_address, obj_name):
        content_object = self.s3.Object(obj_address, obj_name)
        extension = obj_name.split('.')[-1]
        file_content = content_object.get()["Body"].read()
        if len(file_content) == 0:
            raise Exception(f"The object [{obj_address}/{obj_name}] has 0 bytes length.")
        if extension in ('bz2', 'gz'):
            file_content = io.BytesIO(file_content)
        return file_content

    def upload_stage_file(self, filename, obj_name):
        self.upload_file(
            filename=filename,
            obj_address=self._S3_STAGE_BUCKET_NAME,
            obj_name=obj_name)

    def upload_file(self, filename, obj_address, obj_name):
        try:
            s3_object = self.s3.Object(obj_address, obj_name)
            s3_object.upload_file(filename)
        except Exception as error:
            traceback.print_tb(error.__traceback__)
            logger.error(f"Debug: {locals()}")
            raise

    def copy_object(self, source_obj_address, source_obj_name, dest_obj_address, dest_obj_name):
        copy_source = {
            'Bucket': source_obj_address,
            'Key': source_obj_name
        }
        self.s3.meta.client.copy(copy_source, dest_obj_address, dest_obj_name)

    def put_in_saved_data_stream(self, payload):
        self.send_to_stream(
            stream_name=self._KINESIS_SAVED_DATA_STREAM,
            stream_payload=payload)

    def put_in_invalid_data_stream(self, payload):
        self.send_to_stream(
            stream_name=self._KINESIS_INVALID_DATASTREAM,
            stream_payload=payload)

    def put_in_processed_data_stream(self, payload):
        self.send_to_stream(
            stream_name=self._KINESIS_PROCESSED_DATA_STREAM,
            stream_payload=payload)

    def create_event_message(self, event):
        return AWSKinesisEventMessage(event_content=event)
