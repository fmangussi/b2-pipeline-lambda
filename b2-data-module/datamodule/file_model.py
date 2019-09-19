# Database Models
# ---------------------------------------------
# File Model
#
import uuid
import zlib
from datetime import datetime

import pytz

from .config import *
from .product_base import (PARTITION_KEY, SORT_KEY,
                           ProductHelper)
from .utility import get_attr, jsonstr


class ERR_FILE_NOT_FOUND(Exception):
    pass

class ERR_INVALID_RECORD(Exception):
    pass


class FileModel():
    """
    A File that has been processed to create Phases in a Farm
    Supports automatic compression/decompression
    """
    def __init__(self):
        # Machine attributes
        self.customer_id = ""
        self.farm_id = ""
        self.file_id = ""
        self.file_name = ""
        self.file_ext = ""
        self.phases = []
        self.creation_date = ""
        self._content = None

        # Product Helper
        self._ph = ProductHelper(DYN_PRODUCT, AWS_REGION, MODULE_LOGGER)


    @property
    def content(self):
        return zlib.decompress(self._content)

    @content.setter
    def content(self, value):
        self._content = zlib.compress(value, zlib.Z_DEFAULT_COMPRESSION)

    def get_list(self, customer_id, farm_id):
        obj = self._ph.get_files_list(customer_id, farm_id)
        result = []
        for file in obj:
            o = FileModel()
            __, customer_id = file[PARTITION_KEY].split('#')
            __, farm_id, file_id = file[SORT_KEY].split('#')
            o.customer_id = customer_id
            o.farm_id = farm_id
            o.file_id = file_id
            o.file_name = file['file_name']
            o.file_ext = file['file_ext']
            o.phases = file['phases']
            o.creation_date = get_attr(file, 'creation_date')
            result.append(o)
        return result

    def get_list_json(self, customer_id, farm_id):
        obj = self._ph.get_files_list(customer_id, farm_id)
        # print('FILE LIST - ', obj)
        result = []
        for file in obj:
            # print('FILE OBJECT = ', file)
            __, customer_id = file[PARTITION_KEY].split('#')
            __, farm_id, __, file_id = file[SORT_KEY].split('#')
            o = {
                'customer_id': customer_id,
                'farm_id': farm_id,
                'file_id': file_id,
                'file_name': file['file_name'],
                'file_ext': get_attr(file, 'file_ext'),
                'phases': file['phases'],
                'creation_date': get_attr(file, 'creation_date'),
            }
            result.append(o)
        return jsonstr(result)

    def get(self, customer_id, farm_id, file_id):
        obj = self._ph.get_file(customer_id, farm_id, file_id)
        if obj == None:
            raise(ERR_FILE_NOT_FOUND(f"File not found: {customer_id} - {farm_id} - {file_id}"))

        self.customer_id = customer_id
        self.farm_id = farm_id
        self.file_id = file_id
        self.file_name = obj['file_name']
        self.file_ext = obj['file_ext']
        self.creation_date = get_attr(obj, 'creation_date')
        self.phases = get_attr(obj, 'phases', [])
        self._content =  obj['content']

        return self
    
    def new(self,
        customer_id = "",
        farm_id = "",
        file_id = "",
        file_name = "",
        file_ext = "",
        phases = [],
        content = None,
    ):
        self.customer_id = customer_id
        self.farm_id = farm_id
        self.file_id = file_id
        self.file_name = file_name
        self.file_ext = file_ext
        self.phases = phases
        self.content = content

        return self

    def save(self):
        if self.customer_id == "" or self.farm_id == "":
            raise(ERR_INVALID_RECORD("Invalid record, no customer_id/farm_id."))

        if self.file_id == "":    
            # New file, create id
            self.file_id = str(uuid.uuid4()).lower()
            self.creation_date = str(datetime.now(pytz.utc))
        
        # save the file
        file = {
            'file_name': self.file_name,
            'file_ext': self.file_ext,
            'phases': self.phases,
            'creation_date': self.creation_date,
            # 'content': self._content
        }     
        self._ph.put_file(
            self.customer_id.lower(),
            self.farm_id.lower(),
            self.file_id.lower(),
            file_attr=file,
            file_content_attr=self._content
        )   

        return self

    def delete(self):
        self._ph.del_file(self.customer_id, self.farm_id, self.file_id)
