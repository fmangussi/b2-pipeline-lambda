from abc import ABC, abstractmethod

import base64

import json

from helpers import get_logger, UNDEFINED

logger = get_logger(__name__)


class StreamMessageExtraContent:
    _payload = None
    _customer_id = None
    _farm_id = None
    _phase_id = None
    _row_session_id = None
    _machine_id = None
    _hardware_version = None
    _firmware_version = None
    _tag_id = None
    _row_number = None
    _post_number = None
    _side = None
    _direction = None
    _outgoing_timestamp = None

    def __init__(self, **initial_data):
        for key in initial_data:
            setattr(self, key, initial_data[key])

    def get_payload(self):
        return self._payload if self._payload else UNDEFINED

    def get_customer_id(self):
        return self._customer_id if self._customer_id else UNDEFINED

    def get_farm_id(self):
        return self._farm_id if self._farm_id else UNDEFINED

    def get_phase_id(self):
        return self._phase_id if self._phase_id else UNDEFINED

    def get_row_session_id(self):
        return self._row_session_id if self._row_session_id else UNDEFINED

    def get_machine_id(self):
        return self._machine_id if self._machine_id else UNDEFINED

    def get_hardware_version(self):
        return self._hardware_version if self._hardware_version else UNDEFINED

    def get_firmware_version(self):
        return self._firmware_version if self._firmware_version else UNDEFINED

    def get_tag_id(self):
        return self._tag_id if self._tag_id else UNDEFINED

    def get_row_number(self):
        return self._row_number if self._row_number else 0

    def get_post_number(self):
        return self._post_number if self._post_number else 0

    def get_side(self):
        return self._side if self._side else UNDEFINED

    def get_direction(self):
        return self._direction if self._direction else UNDEFINED

    def set_payload(self, value):
        self._payload = value

    def set_customer_id(self, value):
        self._customer_id = value

    def set_farm_id(self, value):
        self._farm_id = value

    def set_phase_id(self, value):
        self._phase_id = value

    def set_row_session_id(self, value):
        self._row_session_id = value

    def set_machine_id(self, value):
        self._machine_id = value

    def set_hardware_version(self, value):
        self._hardware_version = value

    def set_firmware_version(self, value):
        self._firmware_version = value

    def set_tag_id(self, value):
        self._tag_id = value

    def set_row_number(self, value):
        self._row_number = value

    def set_post_number(self, value):
        self._post_number = value

    def set_side(self, value):
        self._side = value

    def set_direction(self, value):
        self._direction = value

    def set_outgoing_timestamp(self, value):
        self._outgoing_timestamp = value

    def get_outgoing_timestamp(self):
        return self._outgoing_timestamp

    def to_dict(self):
        return {
            "outgoing_timestamp": self.get_outgoing_timestamp(),
            "payload" : self.get_payload(),
            "customer_id" : self.get_customer_id(),
            "farm_id" : self.get_farm_id(),
            "phase_id" : self.get_phase_id(),
            "row_session_id" : self.get_row_session_id(),
            "machine_id" : self.get_machine_id(),
            "hardware_version" : self.get_hardware_version(),
            "firmware_version" : self.get_firmware_version(),
            "row_location" : {
                "tag_id": self.get_tag_id(),
                "row_number": self.get_row_number(),
                "post_number": self.get_post_number(),
                "side": self.get_side(),
                "direction": self.get_direction()
            }
        }


class StreamEventMessage(ABC):

    _content = None

    def __init__(self, event_content: str):
        self._content = event_content

    @abstractmethod
    def get_event_data_encoded(self):
        """data in the base64 format"""

    @abstractmethod
    def get_event_data_decoded(self):
        """message decoded from base64 data"""

    @abstractmethod
    def get_event_timestamp_epoch(self):
        """approximateArrivalTimestamp"""

class StreamMessage:


    VERSION = '1.0'

    def __init__(self, dict):
        self._type = None
        self._dict = dict

    @property
    def version(self):
        return self._dict["version"]

    @property
    def source(self):
        return self._dict["source"]

    @property
    def valid(self):
        return self._dict["valid"]

    @property
    def incoming_timestamp(self):
        return self._dict["incoming_timestamp"][:19]

    @property
    def outgoing_timestamp(self):
        return self._dict["outgoing_timestamp"][:19]

    @property
    def original_payload(self):
        return self._dict.get("payload")

    @property
    def list_of_files(self):
        value_payload = self.original_payload
        type_payload = type(value_payload)
        if type_payload is not list and type_payload is str:
            return [value_payload]
        return value_payload

    @property
    def type(self):
        return self._type or self._dict["type"]

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def filename(self):
        return self._dict["filename"]

    def set_invalid(self):
        self._dict["valid"] = False

    def get_base_output(self):
        return {
            "version" : self.VERSION,
            "source": self.source,
            "valid": self.valid,
            "incoming_timestamp": self.incoming_timestamp,
            "outgoing_timestamp": self.outgoing_timestamp,
            "type": self.type,
            "filename": self.filename
        }

    def get_content_output(self, extra: StreamMessageExtraContent):
        if type(extra) is not StreamMessageExtraContent:
            raise Exception("You must implement the StreamMessageExtraContent.")
        base = self.get_base_output()
        base.update(extra.to_dict())
        return base

    def __str__(self):
        return str(self.get_base_output())



class InvalidStreamMessage:
    _version = 1.0
    _source = None
    _valid = False
    _incoming_timestamp = None
    _outgoing_timestamp = None
    #_payload = None
    _type = None
    _filename = None
    _reason = None
    _invalid_source = None
    _stack = None

    @classmethod
    def from_stream_message(cls, stream_message, reason, invalid_source, stack):
        invalid = cls()
        invalid.source = stream_message.source
        invalid.incoming_timestamp = stream_message.incoming_timestamp
        invalid.outgoing_timestamp = stream_message.outgoing_timestamp
        #invalid.payload = stream_message.payload
        invalid.type = stream_message.type
        invalid.filename = stream_message.filename
        invalid.reason = reason
        invalid.invalid_source = invalid_source
        invalid.stack = stack
        return invalid

    def get_version(self):
        return self._version

    def get_source(self):
        return self._source

    def set_source(self, value):
        self._source = value

    def get_valid(self):
        return self._valid

    def get_incoming_timestamp(self):
        result = self._incoming_timestamp
        try:
            if type(result) is not str:
                result = result.strftime("%Y-%m-%d %H:%M:%S.%f")
        finally:
            return result

    def set_incoming_timestamp(self, value):
        self._incoming_timestamp = value

    def get_outgoing_timestamp(self):
        result = self._outgoing_timestamp
        try:
            if type(result) is not str:
                result = result.strftime("%Y-%m-%d %H:%M:%S.%f")
        finally:
            return result

    def set_outgoing_timestamp(self, value):
        self._outgoing_timestamp = value

    def get_type(self):
        return self._type

    def set_type(self, value):
        self._type = value

    def get_filename(self):
        return self._filename

    def set_filename(self, value):
        self._filename = value

    def get_reason(self):
        return self._reason

    def set_reason(self, value):
        self._reason = value

    def get_invalid_source(self):
        return self._invalid_source

    def set_invalid_source(self, value):
        self._invalid_source = value

    def get_stack(self):
        return self._stack

    def set_stack(self, value):
        self._stack = value

    def get_json(self, payload):
        return {
            "version": self.version,
            "source": self.source,
            "valid": self.valid,
            "incoming_timestamp": self.incoming_timestamp,
            "outgoing_timestamp": self.outgoing_timestamp,
            "payload": payload,
            "type": self.type,
            "filename": self.filename,
            "reason": self.reason,
            "invalid_source": self.invalid_source,
            "stack": self.stack
        }

    version = property(get_version)
    source = property(get_source, set_source)
    valid = property(get_valid)
    incoming_timestamp = property(get_incoming_timestamp, set_incoming_timestamp)
    outgoing_timestamp = property(get_outgoing_timestamp, set_outgoing_timestamp)
    #payload = property(get_payload, set_payload)
    type = property(get_type, set_type)
    filename = property(get_filename, set_filename)
    reason = property(get_reason, set_reason)
    invalid_source = property(get_invalid_source, set_invalid_source)
    stack = property(get_stack, set_stack)



class AWSKinesisEventMessage(StreamEventMessage):

    def get_event_data_encoded(self):
        #logger.debug(type(self._content))
        return self._content["kinesis"]["data"]

    def get_event_data_decoded(self):
        return json.loads(
            base64.b64decode(
                self.get_event_data_encoded()).decode('utf-8'))

    def get_event_timestamp_epoch(self):
        return self._content["kinesis"]["approximateArrivalTimestamp"]

class RecordProcessStreamMessage(StreamMessage):
    pass


if __name__ == "__main__":
    event = {
      "kinesis": {
        "COMMENT": "LABEL DATA",
        "kinesisSchemaVersion": "1.0",
        "partitionKey": "2019-04-25 15:45:32",
        "sequenceNumber": "49594210671678830695451044071707598249733934195629097026",
        "data": "ewogICJ2ZXJzaW9uIjogMS4wLAogICJzb3VyY2UiOiAibGFiZWwvODk0Zjk0NDI4YTU2MmJhN2Y1MjRmNDBiNjI4MDI0NzIvMjAxOS0wNC0yMy8wNDM4NzYzMjM1ODkwMDU2LTIwMTkwNDIzLWxlZnQtMTkxMTM0LnRhci5iejIiLAogICJ2YWxpZCI6IHRydWUsCiAgImluY29taW5nX3RpbWVzdGFtcCI6ICIxOTcwLTAxLTAxIDAwOjAwOjAwIiwKICAib3V0Z29pbmdfdGltZXN0YW1wIjogIjIwMTktMDQtMjUgMTU6NDU6MzIiLAogICJ0eXBlIjogImxhYmVsIiwKICAiZmlsZW5hbWUiOiAiMDQzODc2MzIzNTg5MDA1Ni0yMDE5MDQyMy1sZWZ0LTE5MTEzNC5sYWJlbCIsCiAgInBheWxvYWQiOiBbCiAgICAicmF3LXByb2Nlc3Nvci9wcm9jZXNzX2RhdGU9XCIyMDE5LTA1LTE3XCIvZGF0YXR5cGU9XCJhdXhcIi9wcm9jZXNzX2lkPVwiNTgzYjQ4OGQtNzhlOC0xMWU5LTkxYzktMzA5YzIzZTU4MTA4XCIvcmF3LXByb2Nlc3Nvcl9fVEJELTIwMTkwNTA1LWNvbnRyb2xfdW5pdF9pbXUtMjM1OTU5LnRhci5iejJfX1RCRC0yMDE5MDUwNS1jb250cm9sX3VuaXRfaW11LTIzNTk1OS5hdXhfXzU4M2I0ODhkLTc4ZTgtMTFlOS05MWM5LTMwOWMyM2U1ODEwOC5jc3YuZ3oiCiAgXQp9Cg==",
        "approximateArrivalTimestamp": 1556207132.798
      },
      "eventSource": "aws:kinesis",
      "eventVersion": "1.0",
      "eventID": "shardId-000000000004:49594210671678830695451044071707598249733934195629097026",
      "eventName": "aws:kinesis:record",
      "invokeIdentityArn": "arn:aws:iam::425695594515:role/lambda-kinesis-role",
      "awsRegion": "us-west-2",
      "eventSourceARN": "arn:aws:kinesis:us-west-2:425695594515:stream/kinesis-RawDataStream-1R0K6AS5TFUIH"
    }


    m = AWSKinesisEventMessage(event_content=event)
    r = RecordProcessStreamMessage(dict=m.get_event_data_decoded())
    r.list_of_files
    #pprint(str(r))

