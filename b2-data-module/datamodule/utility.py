"""Shared utility helper functions"""
import decimal
import json
import logging
from datetime import datetime

from .config import _KINESIS, _KINESIS_SAVED_DATA_STREAM, _KINESIS_ENABLED


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def jsonstr(obj):
    return json.dumps(obj, cls=DecimalEncoder)

def str_to_bool(s):
    return s.lower() in ['true', '1', 't', 'y', 'yes', '+', 'positive']

def get_attr(obj, attr, default=None):
    if attr in obj:
        return obj[attr]
    else:
        return default

def safe_int(obj, default=0):
    if obj == None:
        return default
    o = str(obj).strip()
    try:
        f = float(o)
    except:
        f = default
    return int(f)    

def convert_type(o, type="str"):
    if o == "" or o is None:
        return o
    type = type.lower()
    if type == "str":
        return str(o).strip()
    elif type == "int":
        return safe_int(o)
    elif type == "int_list":
        lst = str(o).split(",")
        for x in lst:
            x = safe_int(x.strip())
        return lst
    elif type == "float":
        return decimal.Decimal(str(o))
    else:
        return str(o).strip()

def _put_in_kinesis(stream_name, payload):
    if not _KINESIS_ENABLED:
        return
    data = json.dumps(payload)
    return _KINESIS.put_record(
        StreamName=stream_name,
        Data=data,
        PartitionKey = str(datetime.utcnow().replace(microsecond=0)))

def put_in_saved_data_stream(payload):
    try:
        _put_in_kinesis(
            stream_name=_KINESIS_SAVED_DATA_STREAM,
            payload=payload)
    except Exception as ex:
        logging.error("Error writing to Saved Data Stream: Error: <%s>, Msg: <%s>", str(ex), str(payload)[:1000])

def send_update_message(type, data, attribs):
    payload = {
        "version":  "1.00",
        "incoming_timestamp": datetime.utcnow().strftime("%Y/%m/%d %H:%M:%S"),
        "valid": True,
        "type": "update-"+str(type),
        "payload":  data if isinstance(data, str) else jsonstr(data)
    }
    for k, v in attribs.items():
        payload[k] = v
    put_in_saved_data_stream(payload)
