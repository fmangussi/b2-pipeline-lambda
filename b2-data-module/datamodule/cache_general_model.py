# Database Models
# ---------------------------------------------
# Cache General Model
#

import json
from datetime import datetime

import pytz

from .config import *
from .syscache_base import (SysCacheHelper, SORT_KEY, KEY_DELIM,
                            SUB_CMV_AUX, SUB_CMV_COVERAGE, SUB_CMV_FRUITCOUNT, SUB_CMV_LABEL, SUB_CMV_FLOWER_COUNT)
from .utility import send_update_message

type_str = {
    SUB_CMV_AUX: "aux",
    SUB_CMV_COVERAGE: "coverage",
    SUB_CMV_FRUITCOUNT: "fruitcount",
    SUB_CMV_FLOWER_COUNT: 'flowercount',
    SUB_CMV_LABEL: "label"
}


class ERR_CACHE_NOT_FOUND(Exception):
    pass


class ERR_MISSING_KEYS(Exception):
    pass


class CacheGeneralModel():
    """
    General Cache
    """

    def __init__(self, s3_bucket=""):
        # Customer attributes
        self.phase_id = ""
        self.cache_type = ""
        self.date = ""
        self.cache_key = "-"
        self.cache = {}
        self.creation_date = ""

        # Cache Helper
        self._sch = SysCacheHelper(DYN_CACHE, AWS_REGION, MODULE_LOGGER, s3_bucket)

    # def get_list(self, return_null_customer=False):
    #     obj = self._ph.get_customer_list()
    #     result = []
    #     return result

    def get_coverage(self, phase_id, date):
        obj = self._sch.get_cmv(SUB_CMV_COVERAGE, phase_id, date)
        if obj == None:
            raise (ERR_CACHE_NOT_FOUND(f"Coverage cache not found"))

        self.cache_type = SUB_CMV_COVERAGE
        self.phase_id = phase_id
        self.date = date
        try:
            self.cache = json.loads(obj.get('cache_content'))
        except:
            self.cache = obj.get('cache_content')
        self.creation_date = obj.get('creation_date')

        return self

    def get_coverage_dates(self, phase_id, from_date, to_date):
        obj = self._sch.get_cmv_range(SUB_CMV_COVERAGE, phase_id, from_date, to_date)
        if obj == None:
            raise (ERR_CACHE_NOT_FOUND(f"Coverage cache not found"))

        items = []
        for x in obj:
            __, date, __ = x[SORT_KEY].split(KEY_DELIM)
            items.append(date)

        return items

    def get_label(self, phase_id, date):
        obj = self._sch.get_cmv(SUB_CMV_LABEL, phase_id, date)
        if obj == None:
            raise (ERR_CACHE_NOT_FOUND(f"Label cache not found"))

        self.cache_type = SUB_CMV_LABEL
        self.phase_id = phase_id
        self.date = date
        try:
            self.cache = json.loads(obj.get('cache_content'))
        except:
            self.cache = obj.get('cache_content')
        self.creation_date = obj.get('creation_date')

        return self

    def get_aux(self, phase_id, date):
        obj = self._sch.get_cmv(SUB_CMV_AUX, phase_id, date)
        if obj == None:
            raise (ERR_CACHE_NOT_FOUND(f"AUX cache not found"))

        self.cache_type = SUB_CMV_AUX
        self.phase_id = phase_id
        self.date = date
        try:
            self.cache = json.loads(obj.get('cache_content'))
        except:
            self.cache = obj.get('cache_content')
        self.creation_date = obj.get('creation_date')

        return self

    def get_aux_dates(self, phase_id, from_date, to_date):
        obj = self._sch.get_cmv_range(cache_type=SUB_CMV_AUX, phase_id=phase_id, from_date=from_date, to_date=to_date)
        if obj is None:
            raise (ERR_CACHE_NOT_FOUND(f"AUX cache not found"))

        items = []
        for x in obj:
            __, date, __ = x[SORT_KEY].split(KEY_DELIM)
            items.append(date)

        return items

    def get_fruit_count(self, phase_id, date):
        obj = self._sch.get_cmv(SUB_CMV_FRUITCOUNT, phase_id, date)
        if obj == None:
            raise (ERR_CACHE_NOT_FOUND(f"Fruit count cache not found"))

        self.cache_type = SUB_CMV_FRUITCOUNT
        self.phase_id = phase_id
        self.date = date
        try:
            self.cache = json.loads(obj.get('cache_content'))
        except:
            self.cache = obj.get('cache_content')
        self.creation_date = obj.get('creation_date')

        return self

    def get_flower_count(self, phase_id, date):
        obj = self._sch.get_cmv(SUB_CMV_FLOWER_COUNT, phase_id, date)
        if obj is None:
            raise (ERR_CACHE_NOT_FOUND(f"FlowerCount cache not found"))

        self.cache_type = SUB_CMV_FLOWER_COUNT
        self.phase_id = phase_id
        self.date = date
        try:
            self.cache = json.loads(obj.get('cache_content'))
        except:
            self.cache = obj.get('cache_content')
        self.creation_date = obj.get('creation_date')

        return self

    def new(self,
            cache_type="",
            phase_id="",
            date="",
            cache={},
            cache_key="-",
            creation_date=""
            ):
        self.phase_id = phase_id
        self.cache_type = cache_type
        self.date = date
        self.cache_key = cache_key
        self.cache = cache
        self.creation_date = creation_date

        return self

    def new_coverage(self,
                     phase_id="",
                     date="",
                     cache={},
                     ):
        self.new(
            SUB_CMV_COVERAGE,
            phase_id,
            date,
            cache,
        )

    def new_label(self,
                  phase_id="",
                  date="",
                  cache={},
                  ):
        self.new(
            SUB_CMV_LABEL,
            phase_id,
            date,
            cache,
        )

    def new_aux(self,
                phase_id="",
                date="",
                cache={},
                ):
        self.new(
            SUB_CMV_AUX,
            phase_id,
            date,
            cache,
        )

    def new_fruit_count(self,
                        phase_id="",
                        date="",
                        cache={},
                        ):
        self.new(
            SUB_CMV_FRUITCOUNT,
            phase_id,
            date,
            cache,
        )

    def new_flower_count(self,
                         phase_id="",
                         date="",
                         cache=None,
                         ):
        if cache is None:
            cache = dict()
        self.new(
            SUB_CMV_FLOWER_COUNT,
            phase_id,
            date,
            cache,
        )

    def save(self):
        if not self.phase_id or not self.cache_type or not self.date:
            raise ERR_MISSING_KEYS("Cache keys are missing.")

        if not self.creation_date:
            self.creation_date = str(datetime.now(pytz.utc))

        try:
            cache = json.dumps(self.cache)
        except:
            cache = self.cache

        # save the cache
        data = {
            'cache_content': cache,
            'creation_date': self.creation_date,
        }
        self._sch.put_cmv(self.cache_type, self.phase_id, self.date, self.cache_key, data)
        ts = type_str.get(self.cache_type, self.cache_type)

        msg = "REFRESH" if self.cache_type == SUB_CMV_LABEL else cache
        send_update_message(ts, msg, dict(phase_id=self.phase_id, date=self.date))
        return self

    def delete(self):
        self._sch.delete_cmv(self.cache_type, self.phase_id, self.date, self.cache_key)

    def delete_phase(self, cache_type=None, phase_id=None):
        if not cache_type:
            cache_type = self.cache_type
        if not phase_id:
            phase_id = self.phase_id
        self._sch.delete_cmv_phase(cache_type, phase_id)
