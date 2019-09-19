# Database Models
# ---------------------------------------------
# Machine Model
#


import re
import uuid

from .config import *
from .customer_model import CustomerModel
from .product_base import (DATA_STATUS_ARCHIVED, DATA_STATUS_FRESH,
                           DATA_STATUS_INCOMING, DATA_STATUS_PRCOESSED,
                           DATA_STATUS_VALID_VALUES, PARTITION_KEY, SORT_KEY,
                           ProductHelper)
from .sysconfig_base import SysConfigHelper
from .utility import jsonstr, safe_int, get_attr, send_update_message


# Customer crm_id counter
# CUSTOMER_FARMID_COUNTER = 'CUST_CRM_ID'
# CUSTOMER_FARMID_START = 1000

class ERR_MACHINE_NOT_FOUND(Exception):
    pass

class ERR_INVALID_RECORD(Exception):
    pass

class ERR_DELETE(Exception):
    pass

class MachineModel():
    """
    An Ecoation Customer's Machine
    """
    def __init__(self):
        # Machine attributes
        self.customer_id = ""
        self.machine_id = ""
        self.serial_no = ""
        self.machine_type = ""
        self.pin = 0
        self.is_active = ""
        self.data_status = DATA_STATUS_FRESH

        # The machine config set assigned to this machine        
        # the format is 
        # {
        #       config_id: <value>, 
        #       config_name: <value>
        # }
        self.config = {
            'config_id': "",
            'config_name': ""
        }

        # The farms that this machine is assigned to, 
        # the format is [
        #   {
        #       'farm_id': <value>, 
        #       'farm_name': <value>, 
        #       'timezone': <value>
        #   }
        # ]
        self.farms = []     

        # Product Helper
        self._ph = ProductHelper(DYN_PRODUCT, AWS_REGION, MODULE_LOGGER)

        # SysConfig Helper
        self._sc = SysConfigHelper(DYN_CONFIG, AWS_REGION, MODULE_LOGGER)

    def get_list(self, customer_id):
        customer_id = customer_id.lower()
        obj = self._ph.get_customer_machine_list(customer_id)
        result = []

        # Parsing set
        for machine in [m for m in obj if re.match('M#[A-Za-z0-9-]+$', m[SORT_KEY])]:
            o = MachineModel()
            __, machine_id = machine[SORT_KEY].split('#')
            o.customer_id = customer_id
            o.machine_id = machine_id
            o.serial_no = machine['serial_no']
            o.machine_type = machine['machine_type']
            o.pin = safe_int(machine['pin'])
            o.is_active = machine['is_active']
            o.data_status = get_attr(machine, 'data_status', DATA_STATUS_FRESH)

            config = next(iter(item for item in obj if re.match(f'M#{re.escape(o.machine_id)}#CS#[A-Za-z0-9-]+$', item[SORT_KEY])), None)
            if config:
                __, __, __, config_id = config[SORT_KEY].split('#')
                o.config = {
                    'config_id': config_id,
                    'config_name': config['config_name']
                }

            o.farms = []
            for farms in [m for m in obj if re.match(f'M#{re.escape(o.machine_id)}#F#[A-Za-z0-9-]+$', m[SORT_KEY])]:
                __, __, __, farm_id = farms[SORT_KEY].split('#')
                f = {
                    'farm_id': farm_id, 
                    'farm_name': farms['farm_name'], 
                    'timezone': farms['timezone']
                }  
                o.farms.append(f)
            result.append(o)
        return result

    def get_list_all(self):
        '''
        Returns short list of all the machines
        '''
        obj = self._ph.get_all_machines_list()
        result = []

        # Parsing set
        for machine in obj:
            o = MachineModel()
            __, machine_id = machine[SORT_KEY].split('#')
            __, customer_id = machine[PARTITION_KEY].split('#')
            o.customer_id = customer_id
            o.machine_id = machine_id
            o.serial_no = machine['serial_no']
            o.machine_type = machine['machine_type']
            o.pin = safe_int(machine['pin'])
            o.is_active = machine['is_active']
            o.data_status = get_attr(machine, 'data_status', DATA_STATUS_FRESH)
            result.append(o)
        return result

    def get_list_json(self, customer_id):
        obj = self._ph.get_customer_machine_list(customer_id)
        result = []

        # Parsing set
        machines = [m for m in obj if re.match('M#[A-Za-z0-9-_]+$', m[SORT_KEY])]

        for machine in machines:
            o = MachineModel()
            _, machine_id = machine[SORT_KEY].split('#')
            o.customer_id = customer_id
            o.machine_id = machine_id
            o.serial_no = machine['serial_no']
            o.machine_type = machine['machine_type']
            o.pin = safe_int(machine['pin'])
            o.is_active = machine['is_active']
            o.data_status = get_attr(machine, 'data_status', DATA_STATUS_FRESH)

            config = next(iter(item for item in obj if re.match(f'M#{re.escape(o.machine_id)}#CS#[A-Za-z0-9-]+$', item[SORT_KEY])), None)
            if config:
                __, __, __, config_id = config[SORT_KEY].split('#')
                o.config = {
                    'config_id': config_id,
                    'config_name': config['config_name']
                }

            o.farms = []
            for farms in [m for m in obj if re.match(f'M#{re.escape(o.machine_id)}#F#[A-Za-z0-9-]+$', m[SORT_KEY])]:
                __, __, __, farm_id = farms[SORT_KEY].split('#')
                f = {
                    'farm_id': farm_id, 
                    'farm_name': farms['farm_name'], 
                    'timezone': farms['timezone']
                }  
                o.farms.append(f)

            jo = {
                'customer_id': o.customer_id,
                'machine_id': o.machine_id,
                'serial_no': o.serial_no,
                'machine_type': o.machine_type,
                'pin': int(o.pin),
                'is_active': o.is_active,
                'config_id': o.config['config_id'],
                'config_name': o.config['config_name'],
                'farm_id': o.farms[0]['farm_id'] if o.farms and len(o.farms) > 0 else '',
                'farm_name': o.farms[0]['farm_name'] if o.farms and len(o.farms) > 0 else '',
                'data_status': o.data_status
            }
            result.append(jo)

        return jsonstr(result)

    def get(self, customer_id, machine_id):
        obj = self._ph.get_customer_machine_full(customer_id, machine_id)
        if obj == None:
            raise(ERR_MACHINE_NOT_FOUND(f"Machine not found: {customer_id} - {machine_id}"))

        machines = [m for m in obj if re.match('M#[A-Za-z0-9-_]+$', m[SORT_KEY])]
        if len(machines) == 0:
            return None

        machine = machines[0]
        _, machine_id = machine[SORT_KEY].split('#')
        self.customer_id = customer_id
        # print('Self customer:', self.customer_id)
        self.machine_id = machine_id
        self.serial_no = machine['serial_no']
        self.machine_type = machine['machine_type']
        self.pin = safe_int(machine['pin'])
        self.is_active = machine['is_active']
        self.data_status = get_attr(machine, 'data_status', DATA_STATUS_FRESH)

        config = next(iter(item for item in obj if re.match(f'M#{re.escape(machine_id)}#CS#[A-Za-z0-9-]+$', item[SORT_KEY])), None)
        if config:
            __, __, __, config_id = config[SORT_KEY].split('#')
            self.config = {
                'config_id': config_id,
                'config_name': config['config_name']
            }

        self.farms = []
        for f in [m for m in obj if re.match(f'M#{re.escape(machine_id)}#F#[A-Za-z0-9-]+$', m[SORT_KEY])]:
            __, __, __, farm_id = f[SORT_KEY].split('#')
            f = {
                'farm_id': farm_id, 
                'farm_name': f['farm_name'], 
                'timezone': f['timezone']
            }  
            self.farms.append(f)

        return self

    def get_by_id(self, machine_id):
        obj = self._ph.get_machine(machine_id)
        if obj == None:
            raise(ERR_MACHINE_NOT_FOUND(f"Machine not found: {machine_id}"))

        machines = [m for m in obj if re.match('M#[A-Za-z0-9-_]+$', m[SORT_KEY])]
        if len(machines) == 0:
            return None

        machine = machines[0]
        _, machine_id = machine[SORT_KEY].split('#')
        _, customer_id = machine[PARTITION_KEY].split('#')
        self.customer_id = customer_id
        # print('Self customer:', self.customer_id)
        self.machine_id = machine_id
        self.serial_no = machine['serial_no']
        self.machine_type = machine['machine_type']
        self.pin = safe_int(machine['pin'])
        self.is_active = machine['is_active']
        self.data_status = get_attr(machine, 'data_status', DATA_STATUS_FRESH)

        config = next(iter(item for item in obj if re.match(f'M#{re.escape(machine_id)}#CS#[A-Za-z0-9-]+$', item[SORT_KEY])), None)
        if config:
            __, __, __, config_id = config[SORT_KEY].split('#')
            self.config = {
                'config_id': config_id,
                'config_name': config['config_name']
            }

        self.farms = []
        for f in [m for m in obj if re.match(f'M#{re.escape(machine_id)}#F#[A-Za-z0-9-]+$', m[SORT_KEY])]:
            __, __, __, farm_id = f[SORT_KEY].split('#')
            f = {
                'farm_id': farm_id, 
                'farm_name': f['farm_name'], 
                'timezone': f['timezone']
            }  
            self.farms.append(f)

        return self

    def new(self,
        customer_id,
        serial_no = "",
        machine_type = "",
        pin = 0,
        is_active = "",
        config = {
            'config_id': "",
            'config_name': ""
        },
        farms = []     
    ):
        self.customer_id = customer_id
        self.machine_id = ""
        self.serial_no = serial_no
        self.machine_type = machine_type
        self.pin = safe_int(pin)
        self.is_active = is_active
        self.config = config
        self.farms = farms     
        self.data_status = DATA_STATUS_FRESH

        return self

    def save(self):
        if self.customer_id == "":
            raise(ERR_INVALID_RECORD("Invalid record, no customer_id."))

        if self.machine_id == "":    
            # New machine
            self.machine_id = str(uuid.uuid4()).lower()
        
        # save the customer's machine
        machine = {
            'serial_no': self.serial_no,
            'machine_type': self.machine_type,
            'pin': int(self.pin),
            'is_active': self.is_active,
            'data_status': self.data_status,
        }     
        self._ph.put_customer_machine(
            self.customer_id.lower(),
            self.machine_id.lower(), 
            machine_attr=machine,
            machine_config_attr=self.config,
            machine_farms_attr=self.farms
        )   
        send_update_message("machine", [self.get_api_dict()], {})
        return self

    def assign_to_customer(self, new_customer_id):
        self._ph.assign_machine_to_customer(self.customer_id, self.machine_id, new_customer_id)

    def _set_data_status(self, data_status):
        # Used internally, shouldn't be used directly
        if data_status not in DATA_STATUS_VALID_VALUES:
            raise Exception(f'Invalid value: {data_status}')
        self.data_status = data_status
        self._ph.update_customer_machine_data_status(self.customer_id, self.machine_id, data_status)

    def data_incoming(self):
        if DATA_STATUS_INCOMING > self.data_status:
            self._set_data_status(DATA_STATUS_INCOMING)
            cust = CustomerModel()
            try:
                cust.get(self.customer_id)
                cust.data_incoming()
            except:
                pass

    def data_processed(self):
        if DATA_STATUS_PRCOESSED > self.data_status:
            self._set_data_status(DATA_STATUS_PRCOESSED)
            cust = CustomerModel()
            try:
                cust.get(self.customer_id)
                cust.data_processed()
            except:
                pass

    def data_archived(self):
        if DATA_STATUS_ARCHIVED > self.data_status:
            self._set_data_status(DATA_STATUS_ARCHIVED)
            cust = CustomerModel()
            try:
                cust.get(self.customer_id)
                cust.data_archived()
            except:
                pass

    def delete(self):
        if self.data_status != DATA_STATUS_FRESH:
            raise ERR_DELETE(f'Delete failed. Machine {self.machine_id} has associated data: {self.data_status}')
        self._ph.del_customer_machine(self.customer_id, self.machine_id)
        try:
            cst = CustomerModel()
            cst.get(self.customer_id)
            cst.update_message()
        except:
            pass

    def get_api_dict(self):
        Customer=CustomerModel()
        try:
            Customer.get(self.customer_id)
            _crm_id = Customer.crm_id
        except:
            _crm_id = "-"
        x = {
            'cust_id': _crm_id,
            'machine_id': self.machine_id,
            'serial_no': self.serial_no,
            'machine_type': self.machine_type,
            'is_active': self.is_active,
            'config_id': self.config['config_id'],
            'config_name': self.config['config_name'],
            'farm_id': self.farms[0]['farm_id'] if self.farms and len(self.farms) > 0 else '',
            'farm_name': self.farms[0]['farm_name'] if self.farms and len(self.farms) > 0 else '',
        }
        return x

