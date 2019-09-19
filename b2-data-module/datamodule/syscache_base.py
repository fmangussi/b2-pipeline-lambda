# SysConfig (b-c-config) table helper class
#
import copy
import json
import pickle
import sys
import zlib

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from .config import *

# Partition & sort key definitions in SysConfig table
# REF: Configuration Entities - Physical Design
#

# Key Attributes
PARTITION_KEY = 'item_key'
SORT_KEY = 'sort_key'

# Trigger State Entity
ENTITY_TRIGGER_STATE = 'trigger_state'
SUB_TRIGGER_STATE = 'TS'

# Cached Materialized Views Entity
ENTITIY_CMV = 'cv'
SUB_CMV_IMAGE = 'IMG'
SUB_CMV_COVERAGE = 'CVG'
SUB_CMV_LABEL = 'LBL'
SUB_CMV_AUX = 'AUX'
SUB_CMV_FRUITCOUNT = 'FRC'
SUB_CMV_FLOWER_COUNT = 'FLC'

# Processed Files Entity
ENTITY_PROCESSEDFILES = 'pf'

# Key Delimeter
KEY_DELIM = '#'

# Optimistic locking strategy
PUT_ERR_ENTITY_CHANGED = -1
PUT_SUCCESSFUL = 0

# Large object parameters
CMV_ITEM_THRESHOLD = 300000

# Cache types
CMV_CACHE_STANDARD = 'STANDARD'
CMV_CACHE_COMPRESSED = 'COMPRESSED'
CMV_CACHE_S3 = 'S3'

# Errors
class ERR_CACHE_TOO_LARGE(Exception):
    pass

class SysCacheHelper:
    """ 
        SysCache helper class
        USAGE: MySC = SysCacheHelper(table_name, region, app_logger)
            table_name: Name of the SysCache table in DynamoDB
            region: AWS region
            app_logger: Application logger instance
    """

    def __init__(self, table_name, region, app_logger, s3_bucket=""):
        self._table_name = table_name
        self._region = region
        self._app_logger = app_logger
        self._s3_bucket = s3_bucket
        
        self._dynamodb = boto3.resource('dynamodb', region_name=self._region)
        self._s3 = boto3.resource('s3', region_name=self._region)
        self._table = self._dynamodb.Table(self._table_name)
        self._client = boto3.client('dynamodb', region_name=self._region)

    ##########################################################################
    # Trigger State Operations
    ##########################################################################

    def get_trigger_state(self, trigger_key):
        # Read from the database
        pk = ENTITY_TRIGGER_STATE
        sk = self._create_key([SUB_TRIGGER_STATE, trigger_key])
        obj = self._table.get_item(
            Key = {
                PARTITION_KEY: pk,
                SORT_KEY: sk
            },
            ConsistentRead = True,
        )
        if obj == None or 'Item' not in obj or len(obj['Item']) == 0:
            return None
        return obj['Item']

    def put_trigger_state(self, trigger_key, data_attr, prev_version):
        pk = ENTITY_TRIGGER_STATE
        sk = self._create_key([SUB_TRIGGER_STATE, trigger_key])
        # Save new config
        data = copy.deepcopy(data_attr)

        data[PARTITION_KEY] = pk
        data[SORT_KEY] = sk
        # Filling empty strings with '-' to avoid boto3 erros
        Item = { key: value if type(value) is not str or value != '' else '-' for key, value in data.items() } 

        # Writing with support for optimistic locking
        try:
            if prev_version == 0:
                self._table.put_item(
                    Item=Item
                )
            else:
                self._table.put_item(
                    Item=Item,
                    ConditionExpression=Attr('version').eq(prev_version)
                )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return PUT_ERR_ENTITY_CHANGED
            else:
                raise
        except:
            raise
        return PUT_SUCCESSFUL

    def del_trigger_state(self, trigger_key):
        pk = ENTITY_TRIGGER_STATE
        sk = self._create_key([SUB_TRIGGER_STATE, trigger_key])
        self._table.delete_item(        
            Key={
                PARTITION_KEY: pk,
                SORT_KEY: sk
            }
        )
        return

    ##########################################################################
    # Cached Materialized Views Operations
    ##########################################################################

    def cmv_make_cache_data(self, pk, sk, cache_content):
        size = sys.getsizeof(cache_content)
        print(F"Original size: {size/1024}k")
        if size <= CMV_ITEM_THRESHOLD:
            ctype = CMV_CACHE_STANDARD
            content = cache_content
            return (ctype, content)
        else:
            # Compress:
            compressed = zlib.compress(pickle.dumps(cache_content))
            size = sys.getsizeof(compressed)
            print(F"Compressed size: {size/1024}k")
            if size <= CMV_ITEM_THRESHOLD:
                ctype = CMV_CACHE_COMPRESSED
                content = compressed
                return (ctype, content)
            else:
                # Store on s3
                if self._s3_bucket:
                    obj_key = f'cache/{pk}/{sk}.cache'
                    object = self._s3.Object(self._s3_bucket, obj_key)
                    # cache = bytes(pickle.dumps(compressed))
                    cache = bytes(compressed)
                    object.put(Body=cache)
                    ctype = CMV_CACHE_S3
                    content = json.dumps({
                        'bucket': self._s3_bucket,
                        'key': obj_key 
                    })
                    return (ctype, content)
                else:
                    raise ERR_CACHE_TOO_LARGE("The object is too large and S3 bucket is not provided.")

    def cmv_extract_cache_data(self, cache_content_type, cache_content):
        # Get it back:
        if cache_content_type == CMV_CACHE_STANDARD:
            return cache_content
        elif cache_content_type == CMV_CACHE_COMPRESSED:
            # print(str(type(cache_content)))
            decompress = zlib.decompress(cache_content.value)
            obj = pickle.loads(decompress)
            return obj
        elif cache_content_type == CMV_CACHE_S3:
            j = json.loads(cache_content)
            obj = self._s3.Object(j['bucket'], j['key'])
            # cache = pickle.loads(obj.get()['Body'].read())
            cache = obj.get()['Body'].read()
            decompress = zlib.decompress(cache)
            obj = pickle.loads(decompress)
            return obj

    def get_cmv(self, cache_type, phase_id, date, cache_key='-'):
        # Read from the database
        pk = self._create_key([ENTITIY_CMV, phase_id])
        if not cache_key:
            cache_key = '-'
        sk = self._create_key([cache_type, date, cache_key])
        obj = self._table.get_item(
            Key = {
                PARTITION_KEY: pk,
                SORT_KEY: sk
            },
        )
        if obj == None or 'Item' not in obj or len(obj['Item']) == 0:
            return None
        
        # Decode the cache
        obj['Item']['cache_content'] = self.cmv_extract_cache_data(
            obj['Item']['cache_content_type'],
            obj['Item']['cache_content']
        )
        return obj['Item']

    def get_cmv_range(self, cache_type, phase_id, from_date, to_date):
        # Read from the database
        pk = self._create_key([ENTITIY_CMV, phase_id])
        sk1 = self._create_key([cache_type, from_date])
        sk2 = self._create_key([cache_type, to_date])
        
        obj = self.fullquery(
            KeyConditionExpression=Key(PARTITION_KEY).eq(pk) & Key(SORT_KEY).between(sk1, sk2)
        )
        if obj == None:
            return None

        items = []

        for item in obj['Items']:
            # Decode the cache
            item['cache_content'] = self.cmv_extract_cache_data(
                item['cache_content_type'],
                item['cache_content']
            )
            items.append(item)

        return items

    def put_cmv(self, cache_type, phase_id, date, cache_key, data_attr):
        if not cache_key:
            cache_key = "-"
        pk = self._create_key([ENTITIY_CMV, phase_id])
        sk = self._create_key([cache_type, date, cache_key])

        data = copy.deepcopy(data_attr)

        # Support for very large objects
        content_type, content = self.cmv_make_cache_data(pk, sk, data['cache_content'])
        data['cache_content_type'] = content_type
        data['cache_content'] = content

        data[PARTITION_KEY] = pk
        data[SORT_KEY] = sk
        # Filling empty strings with '-' to avoid boto3 erros
        Item = {key: value if type(value) is not str or value != '' else '-' for key, value in data.items() } 
        self._table.put_item(
            Item=Item
        )

    def put_cmv_batch(self, batch):
        '''
        batch: [
            {
                "cache_type": cache_type, 
                "phase_id": phase_id, 
                "date": date, 
                "cache_key": cache_key, 
                "data_attr": data_attr
            }
        ]
        '''

        with self._table.batch_writer() as batch_writer:
            for x in batch:
                try:
                    cache_type = x.get("cache_type")
                    phase_id = x.get("phase_id")
                    date = x.get("date")
                    cache_key = x.get("cache_key")
                    data_attr = x.get("data_attr")
                    if not cache_key:
                        cache_key = "-"
                    pk = self._create_key([ENTITIY_CMV, phase_id])
                    sk = self._create_key([cache_type, date, cache_key])

                    data = copy.deepcopy(data_attr)

                    # Support for very large objects
                    content_type, content = self.cmv_make_cache_data(pk, sk, data['cache_content'])
                    data['cache_content_type'] = content_type
                    data['cache_content'] = content


                    data[PARTITION_KEY] = pk
                    data[SORT_KEY] = sk
                    # Filling empty strings with '-' to avoid boto3 erros
                    Item = {key: value if type(value) is not str or value != '' else '-' for key, value in data.items() } 
                    batch_writer.put_item(
                        Item=Item
                    )
                except Exception as ex:
                    self._app_logger.error("Error while writing batch: str(ex)")

    def delete_cmv(self, cache_type, phase_id, date, cache_key='-'):
        pk = self._create_key([ENTITIY_CMV, phase_id])
        if not cache_key:
            cache_key = '-'
        sk = self._create_key([cache_type, date, cache_key])
        obj = self._table.delete_item(
            Key = {
                PARTITION_KEY: pk,
                SORT_KEY: sk
            },
            ReturnValues='ALL_OLD'
        )
        try:
            content = obj.get('Attributes')
            if content.get('cache_content_type', None) == CMV_CACHE_S3:
                j = json.loads(content.get('cache_content'))
                obj = self._s3.Object(j['bucket'], j['key'])
                obj.delete()
        except:
            self._app_logger.warning(f"Can't delete object {str(content.get('cache_content'))}")

    def delete_cmv_phase(self, cache_type, phase_id):
        pk = self._create_key([ENTITIY_CMV, phase_id])
        if not cache_type:
            cache_type = '-'
        sk = self._create_key([cache_type])
        self.delete_all(pk, sk)

    ##########################################################################
    # Processed Files Operations
    ##########################################################################

    def get_pf(self, bucket, file_name):
        # Read from the database
        pk = self._create_key([ENTITY_PROCESSEDFILES, bucket])
        sk = file_name
        obj = self._table.get_item(
            Key = {
                PARTITION_KEY: pk,
                SORT_KEY: sk
            },
        )
        if obj == None or 'Item' not in obj or len(obj['Item']) == 0:
            return None
        return obj['Item']

    def put_pf(self, bucket, file_name, data_attr):
        pk = self._create_key([ENTITY_PROCESSEDFILES, bucket])
        sk = file_name

        data = copy.deepcopy(data_attr)

        data[PARTITION_KEY] = pk
        data[SORT_KEY] = sk
        # Filling empty strings with '-' to avoid boto3 erros
        Item = {key: value if type(value) is not str or value != '' else '-' for key, value in data.items() } 
        self._table.put_item(
            Item=Item
        )

    def delete_pf(self, bucket, file_name):
        pk = self._create_key([ENTITY_PROCESSEDFILES, bucket])
        sk = file_name
        
        obj = self._table.delete_item(
            Key = {
                PARTITION_KEY: pk,
                SORT_KEY: sk
            },
        )

    ##########################################################################
    # Utitlity Functions
    ##########################################################################

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

