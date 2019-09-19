# Product (b-c-product) table helper class
#
import collections
from base64 import b64encode, b64decode
import copy 

import boto3
from boto3.dynamodb.conditions import Attr, Key

from .config import *
from .utility import get_attr

# Partition & sort key definitions in Product table
# REF: Configuration Entities - Physical Design
#

# Key Attributes
PARTITION_KEY = 'item_key'
SORT_KEY = 'sort_key'

# Etype & Data attribute for customer sub-entities
DATA_ATTRIB = 'data'
ETYPE_ATTRIB = 'etype'

# Customer Entity
ENTITY_CUSTOMER = 'customer'

# null customer
CUSTOMER_NULL_PARTITION_KEY = 'none'

SUB_CUSTOMER_CUSTOMER = 'C'
SUB_CUSTOMER_FARM = 'F'
SUB_CUSTOMER_FARM_SUB_PHASE = 'P'
SUB_CUSTOMER_FARM_SUB_PHASE_SUB_ROW = 'R'
SUB_CUSTOMER_FARM_SUB_FILE = 'FL'
SUB_CUSTOMER_LOCALIZATION = 'L'

SUB_CUSTOMER_MACHINE = 'M'
SUB_CUSTOMER_MACHINE_SUB_CONFIG = 'CS'
SUB_CUSTOMER_MACHINE_SUB_FARM = 'F'

# etype field
SUB_CUSTOMER_CUSTOMER_ETYPE = 'customer'
SUB_CUSTOMER_FARM_ETYPE = 'farm'
SUB_CUSTOMER_FARM_PHASE_ETYPE = 'farm-phase'
SUB_CUSTOMER_FARM_FILE_ETYPE = 'farm-file'
SUB_CUSTOMER_FARM_PHASE_ROW_ETYPE = 'farm-phase-row'
SUB_CUSTOMER_MACHINE_ETYPE = 'machine'
SUB_CUSTOMER_MACHINE_CONFIG_ETYPE = 'machine-config'
SUB_CUSTOMER_MACHINE_FARM_ETYPE = 'machine-farm'

# Config-set Entity
ENTITY_CONFIGSET = 'config-set'
SUB_CONFIGSET_CONFIG = 'CS'
SUB_CONFIGSET_RECIPE = 'CR'
SUB_CONFIGSET_LABELS = 'CL'
SUB_CONFIGSET_CONFIG_ETYPE = 'config-set'
SUB_CONFIGSET_RECIPE_ETYPE = 'config-set-recipe'
SUB_CONFIGSET_LABELS_ETYPE = 'config-set-labels'

# Recipe Entity
ENTITY_RECIPE = 'recipe'
SUB_RECIPE_RECIPE = 'R'
SUB_RECIPE_RECIPE_ETYPE = 'recipe'


# Key Delimeter
KEY_DELIM = '#'

# Data Status Enum
DATA_STATUS_FRESH = '00-fresh'
DATA_STATUS_INCOMING = '10-incoming'
DATA_STATUS_PRCOESSED = '20-processed'
DATA_STATUS_ARCHIVED = '30-archived'
DATA_STATUS_VALID_VALUES = [
    DATA_STATUS_FRESH,
    DATA_STATUS_INCOMING,
    DATA_STATUS_PRCOESSED,
    DATA_STATUS_ARCHIVED,
]

# Phase status
PHASE_STATUS_NONE = ''
PHASE_STATUS_CREATED = '00-created'
PHASE_STATUS_ACTIVE = '10-active'

DEFAULT_ENCODING = 'utf-8'

class EXCEPTION_DUPLICATE_EMAIL(Exception):
    pass

class EXCEPTION_DUPLICATE_TAGID(Exception):
    pass

class EXCEPTION_DUPLICATE_MACHINE_ID(Exception):
    pass

class EXCEPTION_REFERENCE_CHECK(Exception):
    pass

class ProductHelper:
    """ 
        Product helper class
        USAGE: MyPH = ProductHelper(table_name, region, app_logger)
            table_name: Name of the Product table in DynamoDB
            region: AWS region
            app_logger: Application logger instance
    """

    def __init__(self, table_name, region, app_logger):
        self._table_name = table_name
        self._region = region
        self._app_logger = app_logger
        
        self._dynamodb = boto3.resource('dynamodb', region_name=self._region)
        self._table = self._dynamodb.Table(self._table_name)

        # DEBUG check AWS account identity
        # client = boto3.client("sts")
        # print("PRODUCT_BASE: ", client.get_caller_identity())


    ##########################################################################
    # Customer Entity Operations
    ##########################################################################
    def cache_customer(self, customer_id):
        key = self._create_key([ENTITY_CUSTOMER, customer_id])
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(key)
        )
        if 'Items' not in obj:
            return None
        return obj['Items']

    def get_customer(self, customer_id, cache=None):
        if cache == None:
            # Read from the database
            pk = self._create_key([ENTITY_CUSTOMER, customer_id])
            sk = self._create_key([SUB_CUSTOMER_CUSTOMER, customer_id])
            obj = self._table.get_item(
                Key={
                    PARTITION_KEY: pk,
                    SORT_KEY: sk
                }            
            )
            if obj == None or 'Item' not in obj:
                return None
            return obj['Item']
        else:
            # Parse the customer cache
            sk = self._create_key([SUB_CUSTOMER_CUSTOMER, customer_id])
            obj = next(item for item in cache if item[SORT_KEY] == sk)
            return obj

    def get_customer_list(self):
        # Using partial keys to retrieve all items
        # pk = self._create_key([ENTITY_CUSTOMER, ''])
        # sk = self._create_key([SUB_CUSTOMER_CUSTOMER, ''])
        obj = self.fullscan(
            IndexName='customers-only-index',
            # KeyConditionExpression=Key(SORT_KEY).begins_with(sk)
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def get_cusomter_by_crm_id(self, crm_id):
        obj = self.fullquery(
            IndexName='customers-only-index',
            KeyConditionExpression=Key("crm_id").eq(crm_id)
        )
        if 'Items' not in obj or len(obj['Items']) == 0:
            return None
        return obj['Items'][0]

    def check_customer_email(self, customer_id, email):
        obj = self.fullscan(
            ProjectionExpression=PARTITION_KEY,
            IndexName='customers-only-index',
            FilterExpression=Attr('owner_email').eq(email) & 
                Attr(DATA_ATTRIB).ne(customer_id)
        )
        if 'Items' in obj and len(obj['Items']) > 0:
            # Duplicate email
            return False
        else:
            # Email is ok
            return True
    
    def put_customer(self, customer_id, customer_attr):
        attributes = copy.deepcopy(customer_attr)

        email = get_attr(attributes, 'owner_email', None)
        if email and not self.check_customer_email(customer_id, email):
            raise EXCEPTION_DUPLICATE_EMAIL("Email Address is Duplicate.")

        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_CUSTOMER, customer_id])

        attributes[PARTITION_KEY] = pk
        attributes[SORT_KEY] = sk
        # data & etype
        attributes[ETYPE_ATTRIB] = SUB_CUSTOMER_CUSTOMER_ETYPE
        attributes[DATA_ATTRIB] = customer_id
        # Filling empty strings with '-' to avoid boto3 erros
        Item = {key: value if type(value) is not str or value != '' else '-' for key, value in attributes.items() } 
        self._table.put_item(
            Item=Item
        )

    def update_customer_data_status(self, customer_id, new_data_status):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_CUSTOMER, customer_id])
        keys = {
            PARTITION_KEY: pk,
            SORT_KEY: sk
        }
        self.update_data_status(keys, new_data_status)
        
    def del_customer(self, customer_id):
        key = self._create_key([ENTITY_CUSTOMER, customer_id])

        # First, deattach the machines from this customer (we don't want to delete the machines)
        try:
            machines = self.get_customer_machine_list(customer_id)
        except:
            machines = []
        
        for m in machines:
            # Copy machines to a new record with customer assigned to NONE
            m[PARTITION_KEY] = self._create_key([ENTITY_CUSTOMER, CUSTOMER_NULL_PARTITION_KEY])
            self._table.put_item(
                Item=m
            )

        # Now, delete everything!
        self.delete_all(key)

        return

    ##########################################################################
    # Customer/Farm Entity
    ##########################################################################
    def get_customer_farm(self, customer_id, farm_id, cache=None):
        if cache == None:
            # Read from the database
            pk = self._create_key([ENTITY_CUSTOMER, customer_id])
            sk = self._create_key([SUB_CUSTOMER_FARM, farm_id])
            obj = self._table.get_item(
                Key={
                    PARTITION_KEY: pk,
                    SORT_KEY: sk
                }            
            )
            if obj == None or 'Item' not in obj:
                return None
            return obj['Item']
        else:
            # Parse the customer/farm cache
            sk = self._create_key([SUB_CUSTOMER_FARM, farm_id])
            obj = next(item for item in cache if item[SORT_KEY] == sk)
            return obj

    def get_customer_farm_list(self, customer_id):
        # Using partial keys to retrieve all items
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, ''])
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk),
            FilterExpression=Attr(ETYPE_ATTRIB).eq(SUB_CUSTOMER_FARM_ETYPE) 
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def get_all_farms_list(self):
        # Using etype nad entities-index to retrieve all items
        pk = SUB_CUSTOMER_FARM_ETYPE
        obj = self.fullquery(
            IndexName='entities-index',
            KeyConditionExpression=Key(ETYPE_ATTRIB).eq(pk)
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def get_farm_by_farm_id(self, farm_id):
        if not farm_id:
            return None

        # Using etype nad entities-index to retrieve all items
        pk = SUB_CUSTOMER_FARM_ETYPE
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id])
        obj = self.fullquery(
            IndexName='entities-index',
            KeyConditionExpression=Key(ETYPE_ATTRIB).eq(pk),
            FilterExpression=boto3.dynamodb.conditions.Attr(SORT_KEY).eq(sk)
        )
        if obj == None or 'Items' not in obj or len(obj['Items']) == 0:
            return None
        return obj['Items'][0]

    def put_customer_farm(self, customer_id, farm_id, farm_attr):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id])
       
        attributes = copy.deepcopy(farm_attr)

        attributes[PARTITION_KEY] = pk
        attributes[SORT_KEY] = sk
        # data & etype
        attributes[ETYPE_ATTRIB] = SUB_CUSTOMER_FARM_ETYPE
        attributes[DATA_ATTRIB] = farm_id
        # Filling empty strings with '-' to avoid boto3 erros
        Item = {key: value if type(value) is not str or value != '' else '-' for key, value in attributes.items() } 
        self._table.put_item(
            Item=Item
        )

    def update_customer_farm_data_status(self, customer_id, farm_id, new_data_status):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id])
        keys = {
            PARTITION_KEY: pk,
            SORT_KEY: sk
        }
        self.update_data_status(keys, new_data_status)
        
    def del_customer_farm(self, customer_id, farm_id):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id])
        self._table.delete_item(
            Key={
                PARTITION_KEY: pk,
                SORT_KEY: sk
            }
        )
        return

    ##########################################################################
    # Machine Entity
    ##########################################################################
    def get_customer_machine(self, customer_id, machine_id, cache=None):
        if cache == None:
            # Read from the database
            pk = self._create_key([ENTITY_CUSTOMER, customer_id])
            sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])
            obj = self._table.get_item(
                Key={
                    PARTITION_KEY: pk,
                    SORT_KEY: sk
                }            
            )
            if obj == None or 'Item' not in obj:
                return None
            return obj['Item']
        else:
            # Parse the customer/machine cache
            sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])
            obj = next(item for item in cache if item[SORT_KEY] == sk)
            return obj

    # Returns full machine record
    def get_customer_machine_full(self, customer_id, machine_id, cache=None):
        if cache == None:
            # Read from the database
            pk = self._create_key([ENTITY_CUSTOMER, customer_id])
            sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])
            obj = self.fullquery(
                KeyConditionExpression=Key(PARTITION_KEY).eq(pk) & Key(SORT_KEY).begins_with(sk) 
            )
            if obj == None or 'Items' not in obj:
                return None
            return obj['Items']
        else:
            # Parse the customer/machine cache
            sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])
            obj = next(item for item in cache if item[SORT_KEY].starts_with(sk))
            return obj

    def get_customer_machine_list(self, customer_id):
        # Using partial keys to retrieve all items
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_MACHINE, ''])
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk) & Key(SORT_KEY).begins_with(sk) 
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def get_all_machines_list(self):
        # Using etype nad entities-index to retrieve all items
        pk = SUB_CUSTOMER_MACHINE_ETYPE
        obj = self.fullscan(
            IndexName='entities-index',
            FilterExpression=Key(ETYPE_ATTRIB).eq(pk)
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def is_duplicate_machine_id(self, customer_id, machine_id):
        try:
            om = self.fullquery(
                IndexName='entities-index',
                KeyConditionExpression=Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_MACHINE_ETYPE) & Key(DATA_ATTRIB).eq(machine_id)
            )
            if len(om.get('Items', [])) == 0:
                return False
            pk = self._create_key([ENTITY_CUSTOMER, customer_id])
            sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])
            for x in om['Items']:
                if x[SORT_KEY] == sk and x[PARTITION_KEY] != pk:
                    return True
            return False
        except:
            return False

    def put_customer_machine(self, customer_id, machine_id, machine_attr, machine_config_attr=None, machine_farms_attr=[]):
        if self.is_duplicate_machine_id(customer_id, machine_id):
            raise EXCEPTION_DUPLICATE_MACHINE_ID("Invalid record: Duplicate machine id")

        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])

        self.delete_all(pk, sk)

        machine = copy.deepcopy(machine_attr)
        machine_config = copy.deepcopy(machine_config_attr)
        machine_farms = copy.deepcopy(machine_farms_attr)

        machine[PARTITION_KEY] = pk
        machine[SORT_KEY] = sk
        # data & etype
        machine[ETYPE_ATTRIB] = SUB_CUSTOMER_MACHINE_ETYPE
        machine[DATA_ATTRIB] = machine_id
        # Filling empty strings with '-' to avoid boto3 erros
        Item = {key: value if type(value) is not str or value != '' else '-' for key, value in machine.items() } 
        self._table.put_item(
            Item=Item
        )

        if machine_config != None and len(machine_config) > 0 and 'config_id' in machine_config and machine_config['config_id'] != "":
            sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id, SUB_CUSTOMER_MACHINE_SUB_CONFIG, machine_config['config_id']])
            machine_config[PARTITION_KEY] = pk
            machine_config[SORT_KEY] = sk
            config_id = machine_config['config_id']
            del machine_config['config_id']
            # data & etype
            machine_config[ETYPE_ATTRIB] = SUB_CUSTOMER_MACHINE_CONFIG_ETYPE
            machine_config[DATA_ATTRIB] = config_id
            # Filling empty strings with '-' to avoid boto3 erros
            Item = {key: value if type(value) is not str or value != '' else '-' for key, value in machine_config.items() } 
            self._table.put_item(
                Item=Item
            )

        cachefarms = [f['farm_id'] for f in machine_farms]

        for f in machine_farms:
            sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id, SUB_CUSTOMER_MACHINE_SUB_FARM, f['farm_id']])
            f[PARTITION_KEY] = pk
            f[SORT_KEY] = sk
            farm_id = f['farm_id']
            del f['farm_id']
            # data & etype
            f[ETYPE_ATTRIB] = SUB_CUSTOMER_MACHINE_FARM_ETYPE
            f[DATA_ATTRIB] = farm_id
            # Filling empty strings with '-' to avoid boto3 erros
            Item = {key: value if type(value) is not str or value != '' else '-' for key, value in f.items() } 
            self._table.put_item(
                Item=Item
            )
            
    def update_customer_machine_data_status(self, customer_id, machine_id, new_data_status):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])
        keys = {
            PARTITION_KEY: pk,
            SORT_KEY: sk
        }
        self.update_data_status(keys, new_data_status)
        
    def del_customer_machine(self, customer_id, machine_id):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk) & Key(SORT_KEY).begins_with(sk),
            ProjectionExpression=PARTITION_KEY + "," + SORT_KEY
        )
        if obj == None or 'Items' not in obj:
            # No object to delete
            return
        with self._table.batch_writer() as batch:
            for o in obj['Items']:
                batch.delete_item(
                    Key={
                        PARTITION_KEY: o[PARTITION_KEY],
                        SORT_KEY: o[SORT_KEY]
                    }
                )
        return

    def get_machine(self, machine_id):
        key = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])
        obj = self.fullscan(
            IndexName='entities-index',
            FilterExpression=
                (
                    Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_MACHINE_ETYPE) |
                    Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_MACHINE_CONFIG_ETYPE) |
                    Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_MACHINE_FARM_ETYPE)
                ) &
                (
                    Key(SORT_KEY).begins_with(key)
                )
        )
        if 'Items' not in obj:
            return None
        return obj['Items']

    def assign_machine_to_customer(self, customer_id, machine_id, new_customer_id):
        if customer_id == new_customer_id or not customer_id or not machine_id or not new_customer_id:
            return
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_MACHINE, machine_id])
        scan = self.fullscan(
            FilterExpression=Key(PARTITION_KEY).eq(pk) & Key(SORT_KEY).begins_with(sk)
        )

        if 'Items' not in scan or len(scan['Items']) == 0:
            return

        new_key = self._create_key([ENTITY_CUSTOMER, new_customer_id])
        # Copy machines to a new record with customer assigned to new customer
        # Do not copy the assigned farms
        list = [x for x in scan['Items'] if not x[SORT_KEY].startswith(self._create_key([SUB_CUSTOMER_MACHINE, SUB_CUSTOMER_MACHINE_SUB_FARM]))]
        for m in list:
            m[PARTITION_KEY] = new_key
            self._table.put_item(
                Item=m
            )

        # delete old records
        self.delete_all(pk, sk)

    ##########################################################################
    # Config Set Entity
    ##########################################################################

    def get_config_set(self, config_id, cache=None):
        if cache == None:
            # Read from the database
            pk = self._create_key([ENTITY_CONFIGSET, config_id])
            sk = self._create_key([SUB_CONFIGSET_CONFIG, config_id])
            obj = self._table.get_item(
                Key={
                    PARTITION_KEY: pk,
                    SORT_KEY: sk
                }            
            )
            if obj == None or 'Item' not in obj:
                return None
            return obj['Item']
        else:
            # Parse the config set cache
            sk = self._create_key([SUB_CONFIGSET_CONFIG, config_id])
            obj = next(item for item in cache if item[SORT_KEY] == sk)
            return obj

    # Returns full config set record
    def get_config_set_full(self, config_id):
        # Read from the database
        pk = self._create_key([ENTITY_CONFIGSET, config_id])
        # sk = self._create_key([SUB_CONFIGSET_CONFIG, config_id])
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk)
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def get_config_set_list(self):
        # Using configsets-only-index to retrieve all items
        obj = self.fullscan(
            IndexName='configsets-only-index'
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def put_config_set(self, config_id, configset_attr, recipe_attr=None, labels_attr=[]):
        pk = self._create_key([ENTITY_CONFIGSET, config_id])
        sk = self._create_key([SUB_CONFIGSET_CONFIG, config_id])
       
        self.delete_all(pk, sk)
        
        configset = copy.deepcopy(configset_attr)
        recipe = copy.deepcopy(recipe_attr)
        labels = copy.deepcopy(labels_attr)

        configset[PARTITION_KEY] = pk
        configset[SORT_KEY] = sk
        configset[ETYPE_ATTRIB] = SUB_CONFIGSET_CONFIG_ETYPE
        configset[DATA_ATTRIB] = config_id
        # Filling empty strings with '-' to avoid boto3 erros
        Item = {key: value if type(value) is not str or value != '' else '-' for key, value in configset.items() } 
        self._table.put_item(
            Item=Item
        )

        if recipe != None and len(recipe) > 0 and 'recipe_id' in recipe and recipe['recipe_id'] != "":
            sk = self._create_key([SUB_CONFIGSET_RECIPE, recipe['recipe_id']])
            recipe[PARTITION_KEY] = pk
            recipe[SORT_KEY] = sk
            recipe[ETYPE_ATTRIB] = SUB_CONFIGSET_RECIPE_ETYPE
            recipe[DATA_ATTRIB] = recipe['recipe_id']
            del recipe['recipe_id']
            # Filling empty strings with '-' to avoid boto3 erros
            Item = {key: value if type(value) is not str or value != '' else '-' for key, value in recipe.items() } 
            self._table.put_item(
                Item=Item
            )

        for l in labels:
            sk = self._create_key([SUB_CONFIGSET_LABELS, l.label])
            anItem = {
                PARTITION_KEY: pk,
                SORT_KEY: sk,
                'category': l.category,
                ETYPE_ATTRIB: SUB_CONFIGSET_LABELS_ETYPE,
                DATA_ATTRIB: l.label,
            }
            # Filling empty strings with '-' to avoid boto3 erros
            Item = {key: value if type(value) is not str or value != '' else '-' for key, value in anItem.items() } 
            self._table.put_item(
                Item=Item
            )

        # Cross-reference Update: machine-config
        # Using etype and entities-index to retrieve all items
        obj = self.fullquery(
            IndexName='entities-index',
            KeyConditionExpression=Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_MACHINE_CONFIG_ETYPE) & Key(DATA_ATTRIB).eq(config_id)
        )
        if 'Items' in obj:
            try:
                for o in obj['Items']:
                    o['config_name'] = Item['config_name']
                    self._table.put_item(Item=o)
            except:
                pass

    def del_config_set(self, config_id):
        # Cross-reference check: machine-config
        # Raise exception if this config set is used by any machine
        obj = self.fullquery(
            IndexName='entities-index',
            KeyConditionExpression=Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_MACHINE_CONFIG_ETYPE) & Key(DATA_ATTRIB).eq(config_id)
        )
        if 'Items' in obj and len(obj['Items']) > 0:
            raise EXCEPTION_REFERENCE_CHECK("Delete failed, config set is in use.")

        # It is safe to delete
        pk = self._create_key([ENTITY_CONFIGSET, config_id])
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk),
            ProjectionExpression=PARTITION_KEY + "," + SORT_KEY
        )
        if obj == None or 'Items' not in obj:
            # No object to delete
            return
        with self._table.batch_writer() as batch:
            for o in obj['Items']:
                batch.delete_item(
                    Key={
                        PARTITION_KEY: o[PARTITION_KEY],
                        SORT_KEY: o[SORT_KEY]
                    }
                )
        return

    ##########################################################################
    # Recipe Entity
    ##########################################################################

    def get_recipe(self, recipe_id, cache=None):
        if cache == None:
            # Read from the database
            pk = self._create_key([ENTITY_RECIPE, recipe_id])
            sk = self._create_key([SUB_RECIPE_RECIPE, recipe_id])
            obj = self._table.get_item(
                Key={
                    PARTITION_KEY: pk,
                    SORT_KEY: sk
                }            
            )
            if obj == None or 'Item' not in obj:
                return None
            return obj['Item']
        else:
            # Parse the config set cache
            sk = self._create_key([SUB_RECIPE_RECIPE, recipe_id])
            obj = next(item for item in cache if item[SORT_KEY] == sk)
            return obj

    def get_recipe_list(self):
        # Using configsets-only-index to retrieve all items
        obj = self.fullscan(
            IndexName='recipes-only-index'
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def put_recipe(self, recipe_id, recipe_attr):
        pk = self._create_key([ENTITY_RECIPE, recipe_id])
        sk = self._create_key([SUB_RECIPE_RECIPE, recipe_id])
       
        recipe = copy.deepcopy(recipe_attr)
        recipe[PARTITION_KEY] = pk
        recipe[SORT_KEY] = sk
        recipe[ETYPE_ATTRIB] = SUB_RECIPE_RECIPE_ETYPE
        recipe[DATA_ATTRIB] = recipe_id
        # Filling empty strings with '-' to avoid boto3 erros
        Item = {key: value if type(value) is not str or value != '' else '-' for key, value in recipe.items() } 
        self._table.put_item(
            Item=Item
        )

        # Cross-reference Update: config-set-recipe
        # Using etype and entities-index to retrieve all items
        obj = self.fullquery(
            IndexName='entities-index',
            KeyConditionExpression=Key(ETYPE_ATTRIB).eq(SUB_CONFIGSET_RECIPE_ETYPE) & Key(DATA_ATTRIB).eq(recipe_id)
        )
        if 'Items' in obj:
            try:
                for o in obj['Items']:
                    o['recipe_name'] = Item['recipe_name']
                    self._table.put_item(Item=o)
            except:
                pass

    def update_recipe_data_status(self, recipe_id, new_data_status):
        pk = self._create_key([ENTITY_RECIPE, recipe_id])
        sk = self._create_key([SUB_RECIPE_RECIPE, recipe_id])
        keys = {
            PARTITION_KEY: pk,
            SORT_KEY: sk
        }
        self.update_data_status(keys, new_data_status)
        
    def del_recipe(self, recipe_id):
        # Cross-reference check: config-set-recipe
        # Raise exception if this config set is used by any config set
        obj = self.fullquery(
            IndexName='entities-index',
            KeyConditionExpression=Key(ETYPE_ATTRIB).eq(SUB_CONFIGSET_RECIPE_ETYPE) & Key(DATA_ATTRIB).eq(recipe_id)
        )
        if 'Items' in obj and len(obj['Items']) > 0:
            raise EXCEPTION_REFERENCE_CHECK("Delete failed, recipe is in use.")

        # safe to delete
        pk = self._create_key([ENTITY_RECIPE, recipe_id])
        sk = self._create_key([SUB_RECIPE_RECIPE, recipe_id])
        self._table.delete_item(
            Key={
                PARTITION_KEY: pk,
                SORT_KEY: sk
            }
        )
        return

    ##########################################################################
    # Phase Entity
    ##########################################################################
    def get_all_phases_list(self):
        obj = self.fullscan(
            IndexName='entities-index',
            FilterExpression=Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_FARM_PHASE_ETYPE) 
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def get_phases_list(self, *arg):
        farm_ids = list(arg)
        # print("2 - Farms ids:", farm_ids)

        fe_farms = Key(SORT_KEY).eq('-') # Always false to create OR list

        for f in farm_ids:
            k = self._create_key([SUB_CUSTOMER_FARM, f])
            fe_farms = fe_farms | Key(SORT_KEY).begins_with(k)

        fe = Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_FARM_PHASE_ETYPE) & fe_farms

        obj = self.fullscan(
            IndexName='entities-index',
            FilterExpression=fe 
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']
    
    def find_phase(self, phase_id):
        obj = self.fullscan(
            IndexName='entities-index',
            FilterExpression=Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_FARM_PHASE_ETYPE) & Key(DATA_ATTRIB).eq(phase_id)
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def get_customer_phase_full(self, customer_id, farm_id, phase_id, cache=None):
        if cache == None:
            # Read from the database
            pk = self._create_key([ENTITY_CUSTOMER, customer_id])
            sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_PHASE, phase_id])
            obj = self.fullquery(
                KeyConditionExpression=Key(PARTITION_KEY).eq(pk) & Key(SORT_KEY).begins_with(sk) 
            )
            if obj == None or 'Items' not in obj:
                return None
            return obj['Items']
        else:
            # Parse the customer/machine cache
            sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_PHASE, phase_id])
            obj = next(item for item in cache if item[SORT_KEY].starts_with(sk))
            return obj

    def duplicate_tag_ids(self, rows):
        all_tags = [x['tag_id'] for x in rows if x['tag_id'] and x['tag_id'] != '-']
        dups = [item for item, count in collections.Counter(all_tags).items() if count > 1]
        return dups
      
    def check_tag_id(self, phase_key, row_key, tag_id):
        if not tag_id or tag_id == "-":
            return True
        obj = self.fullscan(
            ProjectionExpression=f"{PARTITION_KEY}, {SORT_KEY}, #d",
            IndexName='tag-id-index',
            FilterExpression=Key('tag_id').eq(tag_id),
            ExpressionAttributeNames= {
                "#d": DATA_ATTRIB,
            },
        )
        # print("Obj = ", obj)
        if not 'Items' in obj:
            # tag_id is ok
            return True
        elif len(obj['Items']) == 0:
            # tag_id is ok
            return True
        elif len(obj['Items']) > 1:
            # Duplicate tag_id
            return False
        elif obj['Items'][0][SORT_KEY].startswith(phase_key) and obj['Items'][0][DATA_ATTRIB] == row_key:
            # tag_id is ok
            return True
        else:
            # Duplicate tag_id
            return False
    
    def put_customer_phase(self, customer_id, farm_id, phase_id, phase_attr=None, phase_rows_attr=[], check_rows=True):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_PHASE, phase_id])

        phase = copy.deepcopy(phase_attr)
        phase_rows = copy.deepcopy(phase_rows_attr)

        if check_rows:
            dups = self.duplicate_tag_ids(phase_rows)
            if len(dups) > 0:
                raise(EXCEPTION_DUPLICATE_TAGID(f"Duplicatge tag_id(s) in phase {phase_id}: {dups}."))

            # Validation
            for f in phase_rows:
                # avoid empty and duplicate tag_ids
                if not f['tag_id']:
                    f['tag_id'] = "-"
                if not self.check_tag_id(sk, f['row_key'], f['tag_id']):
                    raise EXCEPTION_DUPLICATE_TAGID(f"tag_id already exists in another phase. This phase {phase_id}: {f['tag_id']}")

        self.delete_all(pk, sk)

        phase[PARTITION_KEY] = pk
        phase[SORT_KEY] = sk
        # data & etype
        phase[ETYPE_ATTRIB] = SUB_CUSTOMER_FARM_PHASE_ETYPE
        phase[DATA_ATTRIB] = phase_id
        # Filling empty strings with '-' to avoid boto3 erros
        Item = {key: value if type(value) is not str or value != '' else '-' for key, value in phase.items() } 
        # print("############## Item = ", Item)
        self._table.put_item(
            Item=Item
        )

        # write all the rows in one batch
        with self._table.batch_writer() as bw:
            for f in phase_rows:
                sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_PHASE, phase_id, SUB_CUSTOMER_FARM_SUB_PHASE_SUB_ROW, f['row_key']])
                f[PARTITION_KEY] = pk
                f[SORT_KEY] = sk
                row_key = f['row_key']
                del f['row_key']
                # data & etype
                f[ETYPE_ATTRIB] = SUB_CUSTOMER_FARM_PHASE_ROW_ETYPE
                f[DATA_ATTRIB] = row_key
                # Filling empty strings with '-' to avoid boto3 erros
                Item = {key: value if type(value) is not str or value != '' else '-' for key, value in f.items() } 
                bw.put_item(
                    Item=Item
                )

    def update_customer_phase_data_status(self, customer_id, farm_id, phase_id, new_data_status):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_PHASE, phase_id])
        keys = {
            PARTITION_KEY: pk,
            SORT_KEY: sk
        }
        self.update_data_status(keys, new_data_status)
        
    def del_customer_farm_phase(self, customer_id, farm_id, phase_id):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_PHASE, phase_id])
        self.delete_all(pk, sk)
        return

    ##########################################################################
    # Row functions
    ##########################################################################

    def get_row_by_tag_id(self, tag_id):
        obj = self.fullquery(
            IndexName='entities-index',
            KeyConditionExpression=Key(ETYPE_ATTRIB).eq(SUB_CUSTOMER_FARM_PHASE_ROW_ETYPE),
            FilterExpression= Attr('tag_id').eq(tag_id)
        )
        if obj == None or 'Items' not in obj or len(obj['Items']) == 0:
            return None
        return obj['Items'][0]


    ##########################################################################
    # File Entity
    ##########################################################################
    def get_files_list(self, customer_id, farm_id):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_FILE])
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk) & Key(SORT_KEY).begins_with(sk),
            ProjectionExpression='item_key, sort_key, file_name, file_ext, phases, creation_date',
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']
    
    def put_file(self, customer_id, farm_id, file_id, file_attr=None, file_content_attr=None):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_FILE, file_id])

        file = copy.deepcopy(file_attr)
        file_content = copy.deepcopy(file_content_attr)

        file[PARTITION_KEY] = pk
        file[SORT_KEY] = sk
        file['content'] = b64encode(file_content).decode(DEFAULT_ENCODING)
        # data & etype
        file[ETYPE_ATTRIB] = SUB_CUSTOMER_FARM_FILE_ETYPE
        file[DATA_ATTRIB] = file_id
        # Filling empty strings with '-' to avoid boto3 erros
        Item = {key: value if type(value) is not str or value != '' else '-' for key, value in file.items() } 
        # print("############## Item = ", Item)
        self._table.put_item(
            Item=Item
        )

    def get_file(self, customer_id, farm_id, file_id, cache=None):
        if cache == None:
            # Read from the database
            pk = self._create_key([ENTITY_CUSTOMER, customer_id])
            sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_FILE, file_id])
            obj = self._table.get_item(
                Key={
                    PARTITION_KEY: pk,
                    SORT_KEY: sk
                }            
            )
            if obj == None or 'Item' not in obj:
                return None
            content = get_attr(obj['Item'], 'content', None)
            obj['Item']['content'] = b64decode(content) if content else None
            # print('DECODED TYPE = ', type(obj['Item']['content']))
            return obj['Item']
        else:
            # Parse the customer/farm cache
            sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_FILE, file_id])
            obj = next(item for item in cache if item[SORT_KEY] == sk)
            return obj

    def del_file(self, customer_id, farm_id, file_id):
        pk = self._create_key([ENTITY_CUSTOMER, customer_id])
        sk = self._create_key([SUB_CUSTOMER_FARM, farm_id, SUB_CUSTOMER_FARM_SUB_FILE, file_id])
        self._table.delete_item(        
            Key={
                PARTITION_KEY: pk,
                SORT_KEY: sk
            }
        )
        return

    ##########################################################################
    # Utility methods
    ##########################################################################
    def _create_key(self, keylist):
        key = keylist[0]
        for k in keylist[1:]:
            key = key + KEY_DELIM + k
        return key

    def delete_all(self, key1, key2=None):
        if key2 == None:
            scan = self.fullscan(
                ProjectionExpression=PARTITION_KEY + ", " + SORT_KEY,
                FilterExpression=Key(PARTITION_KEY).eq(key1)
            )
        else:
            scan = self.fullscan(
                ProjectionExpression=PARTITION_KEY + ", " + SORT_KEY,
                FilterExpression=Key(PARTITION_KEY).eq(key1) & Key(SORT_KEY).begins_with(key2)
            )
        with self._table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(Key={
                    PARTITION_KEY: each[PARTITION_KEY],
                    SORT_KEY: each[SORT_KEY]
                })

    def update_data_status(self, keys, new_data_status):
        response = self._table.update_item(
            Key=keys,
            UpdateExpression="set data_status = :ds",
            ExpressionAttributeValues={
                ':ds': new_data_status,
            },
            ReturnValues="UPDATED_NEW"
        )

    def fullscan(self, **kwargs):
        obj = self._table.scan(**kwargs)
        data = obj["Items"]
        while 'LastEvaluatedKey' in obj: 
            obj = self._table.scan(**kwargs, ExclusiveStartKey=obj['LastEvaluatedKey'])
            data.extend(obj["Items"])
        return {"Items": data}

    def fullquery(self, **kwargs):
        obj = self._table.query(**kwargs)
        data = obj["Items"]
        while 'LastEvaluatedKey' in obj: 
            obj = self._table.query(**kwargs, ExclusiveStartKey=obj['LastEvaluatedKey'])
            data.extend(obj["Items"])
        return {"Items": data}

