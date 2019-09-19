# Database Models
# ---------------------------------------------
# Farm Model
#

import uuid

from .customer_model import CustomerModel
from .config import *
from .product_base import (DATA_STATUS_ARCHIVED, DATA_STATUS_FRESH,
                           DATA_STATUS_INCOMING, DATA_STATUS_PRCOESSED,
                           DATA_STATUS_VALID_VALUES, PARTITION_KEY, SORT_KEY,
                           ProductHelper)
from .sysconfig_base import SysConfigHelper
from .utility import jsonstr, get_attr, send_update_message


# Customer crm_id counter
# CUSTOMER_FARMID_COUNTER = 'CUST_CRM_ID'
# CUSTOMER_FARMID_START = 1000


class ERR_FARM_NOT_FOUND(Exception):
    pass


class ERR_INVALID_RECORD(Exception):
    pass


class ERR_DELETE(Exception):
    pass


class FarmModel():
    """
    An Ecoation Customer's Farm
    """
    def __init__(self):
        # Farm attributes
        self.customer_id = ""
        self.farm_id = ""
        self.farm_name = ""
        self.phone = ""
        self.address = ""
        self.timezone = ""
        self.data_status = DATA_STATUS_FRESH

        # Product Helper
        self._ph = ProductHelper(DYN_PRODUCT, AWS_REGION, MODULE_LOGGER)

        # SysConfig Helper
        self._sc = SysConfigHelper(DYN_CONFIG, AWS_REGION, MODULE_LOGGER)

    def get_list(self, customer_id):
        obj = self._ph.get_customer_farm_list(customer_id)
        result = []
        for farm in obj:
            o = FarmModel()
            __, farm_id = farm[SORT_KEY].split('#')
            o.customer_id = customer_id
            o.farm_id = farm_id
            o.farm_name = farm['farm_name']
            o.phone = farm['phone']
            o.address = farm['address']
            o.timezone = farm['timezone']
            o.data_status = get_attr(farm, 'data_status', DATA_STATUS_FRESH)
            result.append(o)
        return result

    def get_list_all(self):
        obj = self._ph.get_all_farms_list()
        result = []
        for farm in obj:
            o = FarmModel()
            __, farm_id = farm[SORT_KEY].split('#')
            __, customer_id = farm[PARTITION_KEY].split('#')
            o.customer_id = customer_id
            o.farm_id = farm_id
            o.farm_name = farm['farm_name']
            o.phone = farm['phone']
            o.address = farm['address']
            o.timezone = farm['timezone']
            o.data_status = get_attr(farm, 'data_status', DATA_STATUS_FRESH)
            result.append(o)
        return result

    def get_list_json(self, customer_id):
        obj = self._ph.get_customer_farm_list(customer_id)
        result = []
        for farm in obj:
            __, farm_id = farm[SORT_KEY].split('#')
            o = {
                'customer_id': customer_id,
                'farm_id': farm_id,
                'farm_name': farm['farm_name'],
                'phone': farm['phone'],
                'address': farm['address'],
                'timezone': farm['timezone'],
                'data_status': get_attr(farm, 'data_status', DATA_STATUS_FRESH),
            }
            result.append(o)
        return jsonstr(result)

    def get(self, customer_id, farm_id):
        obj = self._ph.get_customer_farm(customer_id, farm_id)
        if obj == None:
            raise(ERR_FARM_NOT_FOUND(f"Farm not found: {customer_id} - {farm_id}"))

        __, farm_id = obj[SORT_KEY].split('#')
        self.customer_id = customer_id
        self.farm_id = farm_id
        self.farm_name = obj['farm_name']
        self.phone = obj['phone']
        self.address = obj['address']
        self.timezone = obj['timezone']
        self.data_status = get_attr(obj, 'data_status', DATA_STATUS_FRESH)

        return self

    def get_by_farm_id(self, farm_id):
        obj = self._ph.get_farm_by_farm_id(farm_id)
        if obj == None:
            raise(ERR_FARM_NOT_FOUND(f"Farm not found: {farm_id}"))

        __, customer_id = obj[PARTITION_KEY].split('#')
        __, farm_id = obj[SORT_KEY].split('#')
        self.customer_id = customer_id
        self.farm_id = farm_id
        self.farm_name = obj['farm_name']
        self.phone = obj['phone']
        self.address = obj['address']
        self.timezone = obj['timezone']
        self.data_status = get_attr(obj, 'data_status', DATA_STATUS_FRESH)

        return self

    def new(self,
        customer_id,
        farm_name = "",
        phone = "",
        address = "",
        timezone = "",
    ):
        self.customer_id = customer_id
        self.farm_id = ""
        self.farm_name = farm_name,
        self.phone = phone,
        self.address = address,
        self.timezone = timezone,
        self.data_status = DATA_STATUS_FRESH

        return self

    def save(self):
        if self.customer_id == "":
            raise(ERR_INVALID_RECORD("Invalid record, no customer_id."))

        if self.farm_id == "":    
            # New farm
            self.farm_id = str(uuid.uuid4()).lower()
        
        # save the customer's farm
        farm = {
            'farm_name': self.farm_name,
            'phone': self.phone,
            'address': self.address,
            'timezone': self.timezone,
            'data_status': self.data_status,
        }     
        self._ph.put_customer_farm(self.customer_id.lower(), self.farm_id.lower(), farm)   
        send_update_message("farm", [self.get_api_dict()], {})
        return self

    def _set_data_status(self, data_status):
        # Used internally, shouldn't be used directly
        if data_status not in DATA_STATUS_VALID_VALUES:
            raise Exception(f'Invalid value: {data_status}')
        self.data_status = data_status
        self._ph.update_customer_farm_data_status(self.customer_id, self.farm_id, data_status)

    def data_incoming(self):
        if DATA_STATUS_INCOMING > self.data_status:
            self._set_data_status(DATA_STATUS_INCOMING)
            from .customer_model import CustomerModel
            cust = CustomerModel()
            try:
                cust.get(self.customer_id)
                cust.data_incoming()
            except:
                pass

    def data_processed(self):
        if DATA_STATUS_PRCOESSED > self.data_status:
            self._set_data_status(DATA_STATUS_PRCOESSED)
            from .customer_model import CustomerModel
            cust = CustomerModel()
            try:
                cust.get(self.customer_id)
                cust.data_processed()
            except:
                pass

    def data_archived(self):
        if DATA_STATUS_ARCHIVED > self.data_status:
            self._set_data_status(DATA_STATUS_ARCHIVED)
            from .customer_model import CustomerModel
            cust = CustomerModel()
            try:
                cust.get(self.customer_id)
                cust.data_archived()
            except:
                pass

    def delete(self):
        if self.data_status != DATA_STATUS_FRESH:
            raise ERR_DELETE(f'Delete failed. Farm {self.farm_id} has associated data: {self.data_status}')
        self._ph.del_customer_farm(self.customer_id, self.farm_id)
        try:
            cst = CustomerModel()
            cst.get(self.customer_id)
            cst.update_message()
        except:
            pass

    def get_api_dict(self):
        from .customer_model import CustomerModel
        from .phase_model import PhaseModel
        Customer=CustomerModel()
        try:
            Customer.get(self.customer_id)
            farm_crm_id = Customer.crm_id
        except:
            farm_crm_id = "-"
        x = {
           "cust_id": int(farm_crm_id),
           "farm_id": self.farm_id,
           "farm_name": self.farm_name,
           "phone": self.phone,
           "address": self.address,
           "timezone": self.timezone,
        }
        Phase = PhaseModel()
        phases = []
        phase_list = Phase.get_list_by_farm_id(self.farm_id)
        if phase_list:
            for p in phase_list:
                phases.append({
                   "phase_id": str(p.phase_id),
                   "phase_name": str(p.phase_name),
                   "phase_type": str(p.phase_type),
                   "creation_date": str(p.creation_date),
                   "is_active": True,
                })
        x["phases"] = phases
        return x