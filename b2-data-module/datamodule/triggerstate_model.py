# Database Models
# ---------------------------------------------
# Trigger State Model
#
# ATTN: This model uses optimistic locking to and string read consistency to tackle concurrency issues

from datetime import datetime

import pytz

from .config import *
from .syscache_base import (PUT_ERR_ENTITY_CHANGED, SysCacheHelper)
from .utility import get_attr


class ERR_TRIGGER_STATE_NOT_FOUND(Exception):
    pass

class ERR_EMPTY_TRIGGER_STATE(Exception):
    pass

# To support optimistic locking, save() method raises this exception 
#   if write fails bacuase the object has been changed in the database since the last read
class ERR_ENTITY_HAS_CHANGED(Exception):
    pass

class ERR_DELETE(Exception):
    pass

class TriggerStateModel():
    """
    A Trigger State Cache Object
    """
    def __init__(self):
        # Customer attributes
        self.state_key = ""
        self.state = ""
        self.creation_date = ""
        self.timeout = int(0)
        self.version = int(0)
        self._prev_version = int(0)

        # SysCache Helper
        self._sch = SysCacheHelper(DYN_CACHE, AWS_REGION, MODULE_LOGGER)

    def get(self, state_key):
        obj = self._sch.get_trigger_state(state_key.lower())
        if obj == None:
            raise(ERR_TRIGGER_STATE_NOT_FOUND(f"Trigger State object not found: {state_key}"))

        self.state_key = state_key
        self.state = get_attr(obj, 'state', '{}')
        self.creation_date = get_attr(obj, 'creation_date')
        self.timeout = int(get_attr(obj, 'timeout', 0))
        self.version = int(get_attr(obj, 'version', 0))

        # Support for optimistic locking
        self._prev_version = self.version
        self.version = self.version + 1

        return self

    def new(self,
        state_key = "",
        state = "",
        creation_date = "",
        timeout = 0,
    ):
        self.state_key = state_key
        self.state = state
        self.creation_date = creation_date
        self.timeout = timeout
        self.version = self._prev_version + 1

        return self

    def save(self):
        if self.state_key == "":
            raise ERR_EMPTY_TRIGGER_STATE("Trigger State object can't be empty")
        
        if not self.creation_date:
            self.creation_date = str(datetime.now(pytz.utc))

        # save the trigger state 
        trg = {
            'state': self.state,
            'creation_date': self.creation_date,
            'timeout': int(self.timeout),
            'version': int(self.version)
        }     

        response = self._sch.put_trigger_state(self.state_key.lower(), trg, self._prev_version)
        if response == PUT_ERR_ENTITY_CHANGED:
            raise ERR_ENTITY_HAS_CHANGED("Optimistic Locking Failed")
        self._prev_version = self.version

        return self

    def delete(self):
        try:
            self._sch.del_trigger_state(self.state_key)
        except Exception as ex:
            raise ERR_DELETE(str(ex))
