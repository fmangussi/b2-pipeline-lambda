import logging
import boto3
import os

########################################################
# AWS config
########################################################
AWS_REGION = "us-west-2"
MODULE_LOGGER = logging.getLogger()
_KINESIS_SAVED_DATA_STREAM = os.environ.get('AWS_KINESIS_SAVED_DATA_STREAM', 'eis-b2-saved-stream')
_KINESIS_ENABLED = True
_KINESIS = boto3.client('kinesis', region_name=AWS_REGION)

def setup_aws(region="us-west-2"):
    global AWS_REGION
    AWS_REGION = region

# DynamoDB Tables
DYN_PRODUCT = "b-c-product"
DYN_CONFIG = "b-c-config"
DYN_CACHE = "b-p-cache"
DYN_LOCK = "b-p-lock"
