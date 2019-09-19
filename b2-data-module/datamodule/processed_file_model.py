# Database Models
# ---------------------------------------------
# Processed File Model
#
import json
from datetime import datetime

import pytz

from .config import *
from .syscache_base import SysCacheHelper


class ERR_FILE_NOT_FOUND(Exception):
    pass

class ERR_INVALID_RECORD(Exception):
    pass

STAGE_RAW_PROCESSED = "raw_processed"
STAGE_RECORD_PROCESSED = "record_processed"
STAGE_ATHENA_WRITTEN = "athena_written"
STAGE_MODEL_PROCESSED = "model_processed"
STAGE_KEYVALUE_STORED = "key_value_processed"
STAGE_INVALID_DUMPED = "invalid_dumped"

class ProcessedFileModel():
    """
    Keeps track of a File that has been processed in the data pipeline.
    """
    def __init__(self):
        # Processed File attributes
        self.bucket = ""
        self.file_name = ""
        self.last_stage = ""
        self.last_stage_datetime = ""
        self.log = []
        self.creation_date = ""

        # Product Helper
        self._sch = SysCacheHelper(DYN_PRODUCT, AWS_REGION, MODULE_LOGGER)

    def get(self, bucket, file_name):
        obj = self._sch.get_pf(bucket, file_name)
        if obj == None:
            raise(ERR_FILE_NOT_FOUND(f"File not found: {bucket} - {file_name}"))

        self.bucket = bucket
        self.file_name = file_name
        self.last_stage = obj['last_stage']
        self.last_stage_datetime = obj['last_stage_datetime']
        try:
            self.log = json.loads(obj["log"])
        except:
            self.log = []

        return self
    
    def new(self,
        bucket = "",
        file_name = "",
        last_stage = "",
        last_stage_datetime = "",
        log = [],
        creation_date = ""
    ):
        self.bucket = bucket
        self.file_name = file_name
        self.last_stage = last_stage
        self.last_stage_datetime = last_stage_datetime
        self.log = log
        self.creation_date = creation_date

        return self

    def save(self):
        if not self.bucket or not self.file_name:
            raise(ERR_INVALID_RECORD("Invalid record, missing keys."))

        if not self.creation_date:    
            self.creation_date = datetime.now(pytz.utc).strftime("%Y/%m/%d %H:%M:%S")
        
        # save the file
        try:
            log = json.dumps(self.log)
        except:
            log = "[]"
        file = {
            "last_stage": self.last_stage,
            "last_stage_datetime": self.last_stage_datetime,
            "log": log,
            "creation_date": self.creation_date,
        }     
        self._sch.put_pf(self.bucket, self.file_name, data_attr=file)

        return self

    def log_stage(self, last_stage, process_name, thetime=datetime.now(pytz.utc)):
        if not self.bucket or not self.file_name:
            raise(ERR_INVALID_RECORD("Invalid record, missing keys."))
        self.last_stage = last_stage
        self.last_stage_datetime = thetime.strftime("%Y/%m/%d %H:%M:%S")
        self.log.append(f"[{self.last_stage_datetime}] [{process_name}] {self.bucket}/{self.file_name} - {self.last_stage}")
        self.save()

    def delete(self):
        self._sch.delete_pf(self.bucket, self.file_name)

