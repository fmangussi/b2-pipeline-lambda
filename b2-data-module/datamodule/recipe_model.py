# Database Models
# ---------------------------------------------
# Recipe Model
#

import uuid
from datetime import datetime

import pytz

from .config import *
from .product_base import (DATA_STATUS_ARCHIVED,
                           DATA_STATUS_FRESH, DATA_STATUS_INCOMING,
                           DATA_STATUS_PRCOESSED, DATA_STATUS_VALID_VALUES,
                           SORT_KEY,
                           ProductHelper)
from .sysconfig_base import SysConfigHelper
from .utility import jsonstr, send_update_message


class ERR_RECIPE_NOT_FOUND(Exception):
    pass

class ERR_INVALID_RECORD(Exception):
    pass

class RecipeModel():
    """
    An Ecoation Recipe
    """
    def __init__(self):
        # Recipe attributes
        self.recipe_id = ""
        self.recipe_name = ""
        self.creation_date = ""
        self.last_recipe_update = ""

        self.sampling_interval_secs = 0

        self.pulse_enable = False
        self.pulse_flash_order = []
        self.pulse_flash_duty_down = 0
        self.pulse_flash_duty_up = 0
        # Map of intensity values for the pulse lights
        self.pulse_intensity = {}
        self.pulse_integration_time = 0
        self.pulse_number_of_scans = 0

        # Map of intensity values for the solid lights
        self.solid_intensity = {}
        self.solid_enable = False
        self.solid_flash_order = []
        self.solid_integration_time = 0
        self.data_status = DATA_STATUS_FRESH

        # Product Helper
        self._ph = ProductHelper(DYN_PRODUCT, AWS_REGION, MODULE_LOGGER)

        # SysConfig Helper
        self._sc = SysConfigHelper(DYN_CONFIG, AWS_REGION, MODULE_LOGGER)

    def get_list(self):
        obj = self._ph.get_recipe_list()
        result = []

        # Parsing set
        for recipe in obj:
            o = RecipeModel()
            __, recipe_id = recipe[SORT_KEY].split('#')
            o.recipe_id = recipe_id
            o.recipe_name = recipe['recipe_name']
            o.creation_date = recipe['creation_date']
            o.last_recipe_update = recipe['last_recipe_update']
            result.append(o)
        return result

    def get_list_json(self):
        obj = self._ph.get_recipe_list()
        result = []

        # Parsing set
        for recipe in obj:
            __, recipe_id = recipe[SORT_KEY].split('#')
            jo = {
                'recipe_id': recipe_id,
                'recipe_name': recipe['recipe_name'],
                'creation_date': recipe['creation_date'],
                'last_recipe_update': recipe['last_recipe_update'],
            }
            result.append(jo)

        return jsonstr(result)

    def get(self, recipe_id):
        obj = self._ph.get_recipe(recipe_id)
        if obj == None:
            raise(ERR_RECIPE_NOT_FOUND(f"Recipe not found: {recipe_id}"))

        __, recipe_id = obj[SORT_KEY].split('#')
        self.recipe_id = recipe_id
        self.recipe_name = obj['recipe_name']
        self.creation_date = obj['creation_date']
        self.last_recipe_update = obj['last_recipe_update']
        self.sampling_interval_secs = obj['sampling_interval_secs']
        self.pulse_enable = obj['pulse_enable']
        self.pulse_flash_order = obj['pulse_flash_order']
        self.pulse_flash_duty_down = obj['pulse_flash_duty_down']
        self.pulse_flash_duty_up = obj['pulse_flash_duty_up']
        self.pulse_intensity = obj['pulse_intensity']
        self.pulse_integration_time = obj['pulse_integration_time']
        self.pulse_number_of_scans = obj['pulse_number_of_scans']
        self.solid_intensity = obj['solid_intensity']
        self.solid_enable = obj['solid_enable']
        self.solid_flash_order = obj['solid_flash_order']
        self.solid_integration_time = obj['solid_integration_time']

    def new(self,
        recipe_name = "",
        creation_date = "",
        last_recipe_update = "",
        sampling_interval_secs = 0,
        pulse_enable = False,
        pulse_flash_order = [],
        pulse_flash_duty_down = 0,
        pulse_flash_duty_up = 0,
        pulse_intensity = {},
        pulse_integration_time = 0,
        pulse_number_of_scans = 0,
        solid_intensity = {},
        solid_enable = False,
        solid_flash_order = [],
        solid_integration_time = 0
    ):
        self.recipe_id = ""
        self.recipe_name = recipe_name
        self.creation_date = creation_date
        self.last_recipe_update = last_recipe_update
        self.sampling_interval_secs = sampling_interval_secs
        self.pulse_enable = pulse_enable
        self.pulse_flash_order = pulse_flash_order
        self.pulse_flash_duty_down = pulse_flash_duty_down
        self.pulse_flash_duty_up = pulse_flash_duty_up
        self.pulse_intensity = pulse_intensity
        self.pulse_integration_time = pulse_integration_time
        self.pulse_number_of_scans = pulse_number_of_scans
        self.solid_intensity = solid_intensity
        self.solid_enable = solid_enable
        self.solid_flash_order = solid_flash_order
        self.solid_integration_time = solid_integration_time
        self.data_status = DATA_STATUS_FRESH

        return self

    def save(self):
        if self.recipe_id == "":    
            # New recipe
            self.recipe_id = str(uuid.uuid4()).lower()
            self.last_recipe_update = self.creation_date = str(datetime.now(pytz.utc))
        else:
            self.last_recipe_update = str(datetime.now(pytz.utc))
        
        # save the recipe
        recipe = {
            'recipe_name': self.recipe_name,
            'creation_date': self.creation_date,
            'last_recipe_update': self.last_recipe_update,
            'sampling_interval_secs': self.sampling_interval_secs,
            'pulse_enable': self.pulse_enable,
            'pulse_flash_order': self.pulse_flash_order,
            'pulse_flash_duty_down': self.pulse_flash_duty_down,
            'pulse_flash_duty_up': self.pulse_flash_duty_up,
            'pulse_intensity': self.pulse_intensity,
            'pulse_integration_time': self.pulse_integration_time,
            'pulse_number_of_scans': self.pulse_number_of_scans,
            'solid_intensity': self.solid_intensity,
            'solid_enable': self.solid_enable,
            'solid_flash_order': self.solid_flash_order,
            'solid_integration_time': self.solid_integration_time
        }     
        self._ph.put_recipe(
            self.recipe_id.lower(),
            recipe
        )   
        send_update_message("phase", [self.get_api_dict()], {})

        return self

    def _set_data_status(self, data_status):
        # Used internally, shouldn't be used directly
        if data_status not in DATA_STATUS_VALID_VALUES:
            raise Exception(f'Invalid value: {data_status}')
        self.data_status = data_status
        self._ph.update_recipe_data_status(self.recipe_id, data_status)

    def data_incoming(self):
        if DATA_STATUS_INCOMING > self.data_status:
            self._set_data_status(DATA_STATUS_INCOMING)

    def data_processed(self):
        if DATA_STATUS_PRCOESSED > self.data_status:
            self._set_data_status(DATA_STATUS_PRCOESSED)

    def data_archived(self):
        if DATA_STATUS_ARCHIVED > self.data_status:
            self._set_data_status(DATA_STATUS_ARCHIVED)

    def delete(self):
        self._ph.del_recipe(self.recipe_id)

    def get_api_dict(self):
        x = {
            'recipe_id': self.recipe_id,
            'recipe_name': self.recipe_name,
            'creation_date': self.creation_date,
            'last_recipe_update': self.last_recipe_update,
        }
        return x

