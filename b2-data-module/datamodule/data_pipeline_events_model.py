# Database Models
# ---------------------------------------------
# Config Set Model
#

import re
import uuid
from datetime import datetime

import pytz

from .config import *
from .product_base import ProductHelper, SORT_KEY
from .sysconfig_base import Label, SysConfigHelper
from .utility import jsonstr, safe_int, get_attr, send_update_message


class ErrConfigSetNotFound(Exception):
    pass


class ERR_INVALID_RECORD(Exception):
    pass


class DummyClass:
    pass


class ConfigSetModel:
    """
    An Ecoation Machine Configuration Set
    """

    def __init__(self):
        # Config Set attributes
        self.config_id = ""
        self.config_name = ""
        self.creation_date = ""
        self.last_config_update = ""

        # Support for sensorSide, binary: bit 0 : left , bit 1 : right
        # Values are: 0-None 1-Left 2-Right 3-Both
        self.sensor_side = 0

        # Aux Sensors
        # the format is
        # [as1, as2, ...]
        self.aux_sensors = []

        # The recipe assigned to this config set
        # the format is 
        # {
        #       'recipe_id': <value>, 
        #       'recipe_name': <value>
        # }
        self.recipe = {
            'recipe_id': "",
            'recipe_name': ""
        }

        # The labels that this configuration set is assigned to, 
        # the label is [
        #   Label
        # ]
        self.labels = []

        # Product Helper
        self._ph = ProductHelper(DYN_PRODUCT, AWS_REGION, MODULE_LOGGER)

        # SysConfig Helper
        self._sc = SysConfigHelper(DYN_CONFIG, AWS_REGION, MODULE_LOGGER)

    def get_list(self, name_only=True):
        obj = self._ph.get_config_set_list()
        result = []

        # Parsing set
        for config in obj:
            __, config_id = config[SORT_KEY].split('#')
            if name_only:
                x = DummyClass()
                x.config_id = config_id
                x.config_name = config['config_name']
                result.append(x)
            else:
                o = ConfigSetModel()
                o.get(config_id)
                result.append(o)
        return result

    def get_list_json(self):
        obj = self._ph.get_config_set_list()
        result = []

        # Parsing set
        for config in obj:
            __, config_id = config[SORT_KEY].split('#')
            o = ConfigSetModel()
            o.get(config_id)
            jo = {
                'config_id': config_id,
                'config_name': o.config_name,
                'creation_date': o.creation_date,
                'last_config_update': o.last_config_update,
                'recipe_name': o.recipe['recipe_name'],
                'recipe_id': o.recipe['recipe_id'],
            }
            result.append(jo)

        return jsonstr(result)

    def get(self, config_id):
        obj = self._ph.get_config_set_full(config_id)
        if obj is None:
            raise (ErrConfigSetNotFound(f"Config set not found: {config_id}"))

        configs = [m for m in obj if re.match(r'CS#[A-Za-z0-9\-]+$', m[SORT_KEY])]
        if len(configs) == 0:
            return None

        config = configs[0]
        __, config_id = config[SORT_KEY].split('#')
        self.config_id = config_id
        self.config_name = config['config_name']
        self.creation_date = config['creation_date']
        self.last_config_update = config['last_config_update']
        self.aux_sensors = config['aux_sensors']
        self.sensor_side = safe_int(get_attr(config, 'sensor_side', 0), 0)
        if self.sensor_side > 3 or self.sensor_side < 0:
            self.sensor_side = 0

        rl = [item for item in obj if re.match(rf'CR#[A-Za-z0-9\-]+$', item[SORT_KEY])]
        if len(rl) > 0:
            recipe = rl[0]
            __, recipe_id = recipe[SORT_KEY].split('#')
            self.recipe = {
                'recipe_id': recipe_id,
                'recipe_name': recipe['recipe_name']
            }

        self.labels = []
        for l in [m for m in obj if re.match(fr'CL#.+$', m[SORT_KEY])]:
            __, label = l[SORT_KEY].split('#')
            lo = Label()
            lo.label = label
            lo.category = l['category']
            self.labels.append(lo)

        return self

    def new(self,
            config_name="",
            creation_date="",
            last_config_update="",
            sensor_side=0,
            aux_sensors=None,
            recipe=None,
            labels=None
            ):
        if labels is None:
            labels = []
        if aux_sensors is None:
            aux_sensors = []
        if recipe is None:
            recipe = {
                'recipe_id': "",
                'recipe_name': ""
            }
        self.config_id = ""
        self.config_name = config_name
        self.creation_date = creation_date
        self.last_config_update = last_config_update
        self.sensor_side = 0
        self.aux_sensors = aux_sensors
        self.recipe = recipe
        self.labels = labels

        return self

    def save(self):
        if self.config_id == "":
            # New config set
            self.config_id = str(uuid.uuid4()).lower()
            self.last_config_update = self.creation_date = str(datetime.now(pytz.utc))
        else:
            self.last_config_update = str(datetime.now(pytz.utc))

        if self.sensor_side > 3 or self.sensor_side < 0:
            self.sensor_side = 0

        # save the Config Set
        config = {
            'config_name': self.config_name,
            'creation_date': self.creation_date,
            'last_config_update': self.last_config_update,
            'aux_sensors': self.aux_sensors,
            'sensor_side': self.sensor_side,
        }
        self._ph.put_config_set(
            self.config_id.lower(),
            config,
            recipe_attr=self.recipe,
            labels_attr=self.labels
        )
        config["receipe"] = self.recipe
        send_update_message("config-set", config, {})

        return self

    def delete(self):
        self._ph.del_config_set(self.config_id)
