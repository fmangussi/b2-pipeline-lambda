# Database Models
# ---------------------------------------------
# Cache Image Model
# caches image paths for fast access
#

from datetime import datetime

import pytz

from .config import *
from .syscache_base import (SysCacheHelper, SUB_CMV_IMAGE)


class ERR_CACHE_NOT_FOUND(Exception):
    pass

class ERR_MISSING_KEYS(Exception):
    pass

class CacheImageModel():
    """
    Image Cache
    """
    def __init__(self):
        # Customer attributes
        self.phase_id = ""
        self.cache_type = ""
        self.date = ""
        self.camera = ""
        self.x = 0
        self.y = 0
        self.image_path = ""
        self.creation_date = ""

        # Cache Helper
        self._sch = SysCacheHelper(DYN_CACHE, AWS_REGION, MODULE_LOGGER)

    # def get_list(self, return_null_customer=False):
    #     obj = self._ph.get_customer_list()
    #     result = []
    #     return result


    def get(self, phase_id, date, camera, x, y):
        cache_key = f"{camera}#{x}#{y}"
        obj = self._sch.get_cmv(SUB_CMV_IMAGE, phase_id, date, cache_key=cache_key)
        if obj == None:
            raise(ERR_CACHE_NOT_FOUND(f"Image cache not found"))

        self.cache_type = SUB_CMV_IMAGE
        self.phase_id = phase_id
        self.date = date
        self.camera = camera
        self.x = x
        self.y = y
        self.image_path = obj.get('cache_content')
        self.creation_date = obj.get('creation_date')

        return self

    def new(self,
        phase_id = "",
        date = "",
        camera = "",
        x = 0,
        y = 0,
        image_path = "",
        creation_date = ""
    ):
        self.phase_id = phase_id
        self.cache_type = SUB_CMV_IMAGE
        self.date = date
        self.camera = camera
        self.x = x
        self.y = y
        self.image_path = image_path
        self.creation_date = creation_date

        return self

    def save(self):
        self.cache_type = SUB_CMV_IMAGE
        if not self.phase_id or not self.cache_type or not self.date:
            raise ERR_MISSING_KEYS("Cache keys are missing.")

        if not self.creation_date:
            self.creation_date = str(datetime.now(pytz.utc))
        
        cache_key = f"{self.camera}#{self.x}#{self.y}"

        # save the cache
        data = {
            'cache_content': self.image_path,
            'creation_date': self.creation_date,
        }     
        self._sch.put_cmv(SUB_CMV_IMAGE, self.phase_id, self.date, cache_key, data)

        return self

    def save_batch(self, batch):
        '''
        batch: a list of CacheImageModel() objects
        '''
        w = []
        for b in batch:
            try:
                cache_type = SUB_CMV_IMAGE
                phase_id = b.phase_id
                date = b.date
                creation_date = b.creation_date
                image_path = b.image_path
                if not phase_id or not cache_type or not date:
                    continue
                if not b.creation_date:
                    creation_date = str(datetime.now(pytz.utc))
                cache_key = f"{b.camera}#{b.x}#{b.y}"

                # save the cache
                data = {
                    'cache_content': image_path,
                    'creation_date': creation_date,
                }
                w.append({
                    "cache_type": SUB_CMV_IMAGE, 
                    "phase_id": phase_id, 
                    "date": date, 
                    "cache_key": cache_key, 
                    "data_attr": data
                })     
            except:
                continue
        self._sch.put_cmv_batch(w)

    def delete(self):
        cache_key = f"{self.camera}#{self.x}#{self.y}"
        self._sch.delete_cmv(SUB_CMV_IMAGE, self.phase_id, self.date, cache_key)

    def delete_phase(self, phase_id=None):
        if not phase_id:
            phase_id = self.phase_id
        self._sch.delete_cmv_phase(SUB_CMV_IMAGE, phase_id)

