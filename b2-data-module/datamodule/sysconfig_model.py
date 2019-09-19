# Database Models
# ---------------------------------------------
# Customer Model
#

import re
from datetime import datetime

import pytz

from .config import *
from .sysconfig_base import SysConfigHelper, PARTITION_KEY, SORT_KEY, CropVariety, Label
from .utility import jsonstr

class ERR_CONFIG_NOT_FOUND(Exception):
    pass

class ERR_INVALID_RECORD(Exception):
    pass

class SysConfigModel():
    """
    System Configuration Model
    """
    def __init__(self):
        # Sys config attributes
        self._old_config_date = None
        self._config_date = None
        self._crops = []
        self._label_categories = []
        self._machine_types = []
        self._account_types = []
        self._pulse_lights = []
        self._solid_lights = []
        self._aux_sensors = []

        # Crop Varieties
        # Format: [
        #   CropVariety Class
        # ]
        self._crop_varieties = None
        self._crop_varieties_changed = False

        # Labels
        # Format: [
        #   Label Class
        # ]
        self._labels = None
        self._labels_changed = False

        # SysConfig Helper
        self._sc = SysConfigHelper(DYN_CONFIG, AWS_REGION, MODULE_LOGGER)

    # Propoerties
    # Using lazy read to access the data only if it is needed

    @property
    def config_date(self):
        return self._config_date


    @property
    def crops(self):
        if self._config_date == None:
            self.read_config()
        return self._crops

    @crops.setter
    def crops(self, value):
        self._crops = value        
        self._config_date = str(datetime.now(pytz.utc))


    @property
    def label_categories(self):
        if self._config_date == None:
            self.read_config()
        return self._label_categories

    @label_categories.setter
    def label_categories(self, value):
        self._label_categories = value        
        self._config_date = str(datetime.now(pytz.utc))


    @property
    def machine_types(self):
        if self._config_date == None:
            self.read_config()
        return self._machine_types

    @machine_types.setter
    def machine_types(self, value):
        self._machine_types = value        
        self._config_date = str(datetime.now(pytz.utc))


    @property
    def account_types(self):
        if self._config_date == None:
            self.read_config()
        return self._account_types

    @account_types.setter
    def account_types(self, value):
        self._account_types = value        
        self._config_date = str(datetime.now(pytz.utc))


    @property
    def pulse_lights(self):
        if self._config_date == None:
            self.read_config()
        return self._pulse_lights

    @pulse_lights.setter
    def pulse_lights(self, value):
        self._pulse_lights = value        
        self._config_date = str(datetime.now(pytz.utc))


    @property
    def solid_lights(self):
        if self._config_date == None:
            self.read_config()
        return self._solid_lights

    @solid_lights.setter
    def solid_lights(self, value):
        self._solid_lights = value        
        self._config_date = str(datetime.now(pytz.utc))


    @property
    def aux_sensors(self):
        if self._config_date == None:
            self.read_config()
        return self._aux_sensors

    @aux_sensors.setter
    def aux_sensors(self, value):
        self._aux_sensors = value        
        self._config_date = str(datetime.now(pytz.utc))


    @property
    def crop_varieties(self):
        if self._crop_varieties == None:
            self.read_crop_varieties()
        return self._crop_varieties

    @crop_varieties.setter
    def crop_varieties(self, value):
        if self._crop_varieties != value:
            self._crop_varieties = value        
            self._crop_varieties_changed = True


    @property
    def labels(self):
        if self._labels == None:
            self.read_labels()
        return self._labels

    @labels.setter
    def labels(self, value):
        if self._labels != value:
            self._labels = value        
            self._labels_changed = True
    
    # Database operations

    def read_config(self):
        config = self._sc.get_sys_config()
        if config == None:
            self.__init__()
            return

        __, config_date = config[SORT_KEY].split('#')
        self._old_config_date = config_date
        self._config_date = config_date
        self._crops = config['crops']
        self._label_categories = config['label_categories']
        self._machine_types = config['machine_types']
        self._account_types = config['account_types']
        self._pulse_lights = config['pulse_lights']
        self._solid_lights = config['solid_lights']
        self._aux_sensors = config['aux_sensors']

        return

    def read_crop_varieties(self):
        crop_varieties = self._sc.get_crop_varieties()
        result = []
        for cv in crop_varieties:
            __, variety = cv[SORT_KEY].split('#')
            o = CropVariety()
            o.variety = variety
            o.crop = cv['crop']
            result.append(o)
        result.sort(key = lambda x: x.crop.lower())
        self._crop_varieties = result
        return result
        
    def read_labels(self):
        labels = self._sc.get_labels()
        result = []
        for l in labels:
            __, label = l[SORT_KEY].split('#')
            o = Label()
            o.label = label
            o.category = l['category']
            result.append(o)
        result.sort(key = lambda x: x.category.lower())
        self._labels = result
        return result

    def get_all(self):
        self.read_config()
        self.read_crop_varieties()
        self.read_labels()
        return

    def save(self):
        if self._config_date != self._old_config_date:
            # Writing new sys config values
            config = {
                'crops': self._crops,
                'label_categories': self._label_categories,
                'machine_types': self._machine_types,
                'account_types': self._account_types,
                'pulse_lights': self._pulse_lights,
                'solid_lights': self._solid_lights,
                'aux_sensors': self._aux_sensors
            }
            self._sc.put_sys_config(config)

        if self._crop_varieties_changed:
            self._sc.put_crop_varieties(self._crop_varieties)

        if self._labels_changed:
            self._sc.put_labels(self._labels)

        return self
