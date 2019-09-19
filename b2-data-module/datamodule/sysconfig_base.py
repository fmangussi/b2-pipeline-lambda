# SysConfig (b-c-config) table helper class
#
import copy
from datetime import datetime

import pytz
from boto3.dynamodb.conditions import Key

from .config import *

# Partition & sort key definitions in SysConfig table
# REF: Configuration Entities - Physical Design
#

# Key Attributes
PARTITION_KEY = 'config_type'
SORT_KEY = 'config_key'

# SysConfig Entity
ENTITY_SYSCONFIG = 'sys'

SUB_SYSCONFIG_LATEST = 'latest'
SUB_SYSCONFIG_HISTORY = 'history'

# CropVariety Entity
ENTITY_CROPVARIETY = 'crop_variety'

SUB_CROPVARIETY_CROPVARIETY = 'C'

# Label Entity
ENTITY_LABEL = 'label'

SUB_LABEL_LABEL = 'L'

# Counter Entity
ENTITY_COUNTER = 'counter'

SUB_COUNTER_COUNTER = 'CNT'
ATTRIB_COUNTER_VALUE = 'counter_value'

# Key Delimeter
KEY_DELIM = '#'

# Helper classes

class CropVariety():
    """
    Crop Variety Class
    """
    def __init__(self):
        self.variety = ""
        self.crop = ""

class Label():
    """
    Label Class
    """
    def __init__(self):
        self.label = ""
        self.category = ""

class SysConfigHelper:
    """ 
        SysConfig helper class
        USAGE: MySC = SysConfigHelper(table_name, region, app_logger)
            table_name: Name of the SysConfig table in DynamoDB
            region: AWS region
            app_logger: Application logger instance
    """

    def __init__(self, table_name, region, app_logger):
        self._table_name = table_name
        self._region = region
        self._app_logger = app_logger
        
        self._dynamodb = boto3.resource('dynamodb', region_name=self._region)
        self._table = self._dynamodb.Table(self._table_name)

    def cache_config(self):
        obj = self._table.scan()
        if 'Items' not in obj:
            return None
        return obj['Items']

    
    ##########################################################################
    # Sys Config Operations
    ##########################################################################

    def get_sys_config(self):
        # Read from the database
        pk = ENTITY_SYSCONFIG
        sk = SUB_SYSCONFIG_LATEST
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk) & Key(SORT_KEY).begins_with(sk)
        )
        if obj == None or 'Items' not in obj or len(obj['Items']) == 0:
            return None
        return obj['Items'][0]

    def get_crop_varieties(self):
        # Read from the database
        pk = ENTITY_CROPVARIETY
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk)
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def get_labels(self):
        # Read from the database
        pk = ENTITY_LABEL
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk)
        )
        if obj == None or 'Items' not in obj:
            return None
        return obj['Items']

    def put_sys_config(self, config_attr):
        pk = ENTITY_SYSCONFIG
        sk = SUB_SYSCONFIG_LATEST
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk) & Key(SORT_KEY).begins_with(sk),
        )

        config = copy.deepcopy(config_attr)

        if obj and 'Items' in obj and len(obj['Items']) > 0:
            # Put the previous config in history        
            Item = obj['Items'][0]
            _, old_date = Item[SORT_KEY].split('#')
            old_key = Item[SORT_KEY]
            new_key = self._create_key([SUB_SYSCONFIG_HISTORY, old_date])

            with self._table.batch_writer() as batch:
                batch.delete_item(
                    Key={
                        PARTITION_KEY: pk,
                        SORT_KEY: old_key
                    }
                )
                Item[PARTITION_KEY] = pk
                Item[SORT_KEY] = new_key
                batch.put_item(
                    Item=Item
                )

        # Save new config
        date = str(datetime.now(pytz.utc))
        sk = self._create_key([SUB_SYSCONFIG_LATEST, date])
        config[PARTITION_KEY] = pk
        config[SORT_KEY] = sk
        # Filling empty strings with '-' to avoid boto3 erros
        Item = { key: value if type(value) is not str or value != '' else '-' for key, value in config.items() } 
        self._table.put_item(
            Item=Item
        )

    def put_crop_varieties(self, crops_attr):
        pk = ENTITY_CROPVARIETY
        self.delete_all(pk)

        crops = copy.deepcopy(crops_attr)

        with self._table.batch_writer() as batch:
            for c in crops:
                Item = {
                    PARTITION_KEY: pk,
                    SORT_KEY: self._create_key([SUB_CROPVARIETY_CROPVARIETY, c.variety]),
                    'crop': c.crop
                }
                batch.put_item(
                    Item=Item
                )

    def put_labels(self, labels_attr):
        pk = ENTITY_LABEL
        self.delete_all(pk)

        labels = copy.deepcopy(labels_attr)

        with self._table.batch_writer() as batch:
            for l in labels:
                Item = {
                    PARTITION_KEY: pk,
                    SORT_KEY: self._create_key([SUB_LABEL_LABEL, l.label]),
                    'category': l.category
                }
                batch.put_item(
                    Item=Item
                )

    ##########################################################################
    # Counters
    ##########################################################################

    def get_new_counter_value(self, counter_name, increment, starting_value=0):
        pk = ENTITY_COUNTER
        sk = self._create_key([SUB_COUNTER_COUNTER, counter_name])

        try:
            obj = self._table.update_item(
                ReturnValues="UPDATED_NEW",
                ExpressionAttributeValues={
                    ":a": increment
                },
                ExpressionAttributeNames={
                    "#v": ATTRIB_COUNTER_VALUE
                },
                UpdateExpression="SET #v = #v + :a",
                Key={
                    PARTITION_KEY: pk,
                    SORT_KEY: sk
                }
            )
            if 'Attributes' in obj:
                if ATTRIB_COUNTER_VALUE in obj['Attributes']:
                    return int(obj['Attributes'][ATTRIB_COUNTER_VALUE])
        except:
            # The counter doesn't exist, create the counter
            try:
                self._table.put_item ( 
                    Item={
                        PARTITION_KEY: pk,
                        SORT_KEY: sk,
                        ATTRIB_COUNTER_VALUE: starting_value
                    }
                )
                return starting_value
            except Exception as x:
                raise(x)

    ##########################################################################
    # Utitlity Functions
    ##########################################################################

    def delete_all(self, the_key):
        scan = self._table.scan(
            ProjectionExpression=PARTITION_KEY + ", " + SORT_KEY,
            FilterExpression=Key(PARTITION_KEY).eq(the_key)
        )
        with self._table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(Key=each)


    def _create_key(self, keylist):
        key = keylist[0]
        for k in keylist[1:]:
            key = key + KEY_DELIM + k
        return key

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

