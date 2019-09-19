# Database Models
# ---------------------------------------------
# Customer Model
#

import uuid
from datetime import datetime

import pytz

from .config import *
from .product_base import (DATA_STATUS_ARCHIVED, DATA_STATUS_FRESH,
                           DATA_STATUS_INCOMING, DATA_STATUS_PRCOESSED,
                           DATA_STATUS_VALID_VALUES, SORT_KEY,
                           ProductHelper, CUSTOMER_NULL_PARTITION_KEY)
from .sysconfig_base import SysConfigHelper
from .utility import get_attr, jsonstr, send_update_message

# Customer crm_id counter
CUSTOMER_CRMID_COUNTER = 'CUST_CRM_ID'
CUSTOMER_CRMID_START = 1000

class ERR_CUSTOMER_NOT_FOUND(Exception):
    pass

class ERR_GETTING_CRM_ID(Exception):
    pass

class ERR_DELETE(Exception):
    pass

class CustomerModel():
    """
    An Ecoation Customer
    """
    def __init__(self):
        # Customer attributes
        self.customer_id = ""
        self.customer_name = ""
        self.crm_id = int(0)
        self.alias = ""
        self.phone = ""
        self.address = ""
        self.logo = ""
        self.creation_date = ""
        self.default_timezone = ""
        self.account_type = "Standard"
        self.owner_email = ""
        self.is_active = False
        self.data_status = DATA_STATUS_FRESH

        # Product Helper
        self._ph = ProductHelper(DYN_PRODUCT, AWS_REGION, MODULE_LOGGER)

        # SysConfig Helper
        self._sc = SysConfigHelper(DYN_CONFIG, AWS_REGION, MODULE_LOGGER)

    def get_list(self, return_null_customer=False):
        obj = self._ph.get_customer_list()
        result = []
        for cust in obj:
            if not return_null_customer and cust[SORT_KEY] == CUSTOMER_NULL_PARTITION_KEY:
                continue
            o = CustomerModel()
            __, customer_id = cust[SORT_KEY].split('#')
            o.customer_id = customer_id
            o.customer_name = get_attr(cust, 'customer_name')
            o.crm_id = int(get_attr(cust, 'crm_id'))
            o.alias = get_attr(cust, 'alias')
            o.phone = get_attr(cust, 'phone')
            o.address = get_attr(cust, 'address')
            o.logo = get_attr(cust, 'logo')
            o.creation_date = get_attr(cust, 'creation_date')
            o.default_timezone = get_attr(cust, 'default_timezone')
            o.account_type = get_attr(cust, 'account_type')
            o.owner_email = get_attr(cust, 'owner_email')
            o.is_active = get_attr(cust, 'is_active')
            o.data_status = get_attr(cust, 'data_status', DATA_STATUS_FRESH)
            result.append(o)
        return result

    def get_list_json(self, return_null_customer=False):
        obj = self._ph.get_customer_list()
        result = []
        for cust in obj:
            if not return_null_customer and cust[SORT_KEY] == CUSTOMER_NULL_PARTITION_KEY:
                continue
            __, customer_id = cust[SORT_KEY].split('#')
            o = {
                'customer_id': customer_id,
                'customer_name': get_attr(cust, 'customer_name'),
                'crm_id': int(get_attr(cust, 'crm_id')),
                'alias': get_attr(cust, 'alias'),
                'phone': get_attr(cust, 'phone'),
                'address': get_attr(cust, 'address'),
                'logo': get_attr(cust, 'logo'),
                'creation_date': get_attr(cust, 'creation_date'),
                'default_timezone': get_attr(cust, 'default_timezone'),
                'account_type': get_attr(cust, 'account_type'),
                'owner_email': get_attr(cust, 'owner_email'),
                'is_active': get_attr(cust, 'is_active'),
                'data_status': get_attr(cust, 'data_status', DATA_STATUS_FRESH),
            }
            result.append(o)
        return jsonstr(result)

    def get(self, customer_id):
        obj = self._ph.get_customer(customer_id)
        if obj == None:
            raise(ERR_CUSTOMER_NOT_FOUND(f"Customer not found: {customer_id}"))

        __, customer_id = obj[SORT_KEY].split('#')
        self.customer_id = customer_id
        self.customer_name = get_attr(obj, 'customer_name')
        self.crm_id = int(get_attr(obj, 'crm_id'))
        self.alias = get_attr(obj, 'alias')
        self.phone = get_attr(obj, 'phone')
        self.address = get_attr(obj, 'address')
        self.logo = get_attr(obj, 'logo')
        self.creation_date = get_attr(obj, 'creation_date')
        self.default_timezone = get_attr(obj, 'default_timezone')
        self.account_type = get_attr(obj, 'account_type')
        self.owner_email = get_attr(obj, 'owner_email')
        self.is_active = get_attr(obj, 'is_active')
        self.data_status = get_attr(obj, 'data_status', DATA_STATUS_FRESH)

        return self

    def get_by_crm_id(self, crm_id):
        obj = self._ph.get_cusomter_by_crm_id(int(crm_id))
        if obj == None:
            raise(ERR_CUSTOMER_NOT_FOUND(f"Customer not found, CRM_ID: {crm_id}"))

        __, customer_id = obj[SORT_KEY].split('#')
        self.customer_id = customer_id
        self.customer_name = get_attr(obj, 'customer_name')
        self.crm_id = int(get_attr(obj, 'crm_id'))
        self.alias = get_attr(obj, 'alias')
        self.phone = get_attr(obj, 'phone')
        self.address = get_attr(obj, 'address')
        self.logo = get_attr(obj, 'logo')
        self.creation_date = get_attr(obj, 'creation_date')
        self.default_timezone = get_attr(obj, 'default_timezone')
        self.account_type = get_attr(obj, 'account_type')
        self.owner_email = get_attr(obj, 'owner_email')
        self.is_active = get_attr(obj, 'is_active')
        self.data_status = get_attr(obj, 'data_status', DATA_STATUS_FRESH)

        return self

    def new(self,
        customer_name = "",
        alias = "",
        phone = "",
        address = "",
        logo = "",
        default_timezone = "",
        account_type = "Standard",
        owner_email = "",
        is_active = False
    ):
        self.customer_id = ""
        self.crm_id = int(0)
        self.customer_name = customer_name,
        self.alias = alias,
        self.phone = phone,
        self.address = address,
        self.logo = logo,
        self.creation_date = "",
        self.default_timezone = default_timezone,
        self.account_type = account_type
        self.owner_email = owner_email
        self.is_active = is_active
        self.data_status = DATA_STATUS_FRESH

        return self

    def save(self):
        if self.customer_id == "":
            # New customer
            crmid = self._sc.get_new_counter_value(
                CUSTOMER_CRMID_COUNTER, 
                increment=1, 
                starting_value=CUSTOMER_CRMID_START
            )
            if crmid == None:
                raise(ERR_GETTING_CRM_ID("Can't get crm_id"))
            self.customer_id = str(uuid.uuid4()).lower()
            self.crm_id = int(crmid)
            self.creation_date = str(datetime.now(pytz.utc))
        
        # save the customer
        cust = {
            'customer_name': self.customer_name,
            'crm_id': self.crm_id,
            'alias': self.alias,
            'phone': self.phone,
            'address': self.address,
            'logo': self.logo,
            'creation_date': self.creation_date,
            'default_timezone': self.default_timezone,
            'account_type': self.account_type,
            'owner_email': self.owner_email.lower(),
            'is_active': self.is_active,
            'data_status': self.data_status,
        }     
        self._ph.put_customer(self.customer_id.lower(), cust)   
        self.update_message()
        return self

    def _set_data_status(self, data_status):
        # Used internally, shouldn't be used directly
        if data_status not in DATA_STATUS_VALID_VALUES:
            raise Exception(f'Invalid value: {data_status}')
        self.data_status = data_status
        self._ph.update_customer_data_status(self.customer_id, data_status)

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
        if self.data_status != DATA_STATUS_FRESH:
            raise ERR_DELETE(f'Delete failed. Customer {self.customer_id} has associated data: {self.data_status}')
        self._ph.del_customer(self.customer_id)
        self.update_message()

    def update_message(self):
        send_update_message("customer", [self.get_api_dict()], 
            {
                'cust_id': self.crm_id
            }
        )

    def get_api_dict(self):
        x = {
            "cust_id": int(self.crm_id),
            "customer_name": self.customer_name,
            "alias": self.alias,
            "phone": self.phone,
            "address": self.address,
            "creation_date": self.creation_date,
            "default_timezone": self.default_timezone,
            "account_type": self.account_type,
            "owner_email": self.owner_email,
            "is_active": self.is_active,
        } 
        
        from .farm_model import FarmModel
        Farm = FarmModel()
        farms = []
        farm_list = Farm.get_list(self.customer_id)
        if farm_list:
            for f in farm_list:
                farms.append({
                    "farm_id": f.farm_id,
                    "farm_name": f.farm_name,
                })
        x["farms"] = farms
        return x