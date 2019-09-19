from abc import ABC, abstractmethod

from datetime import datetime

from helpers import get_logger, UNDEFINED, cast_to_int

logger = get_logger(__name__)


class DataLakeRecord(ABC):

    """Represent the attributes for Data Lake Record
  
    It was implemented because the cardinality between input and ouput
    is different for each file type.

    For example:
        Wave:
            has multiples lineas that are only metadata

        Label:
            each line will generate multiples outputs

        Video:
            each frame will generate one output

    """

    VERSION = '1.0'
    _json_line: str
    post_length_cache = {}

    def __init__(self, record_processor):
        self._record_processor = record_processor
        self._is_valid = True

    @staticmethod
    def handle_pandas_null_values(value, default):
        if str(value) == 'nan' or value is None:
            return default
        return value

    def invalidate(self, reason, invalid_source, stack):
        self._is_valid = False

    def set_its_valid(self):
        self._is_valid = True

    @property
    def is_valid(self):
        return self._is_valid

    @property
    def record_processor(self):
        return self._record_processor

    @property
    def json_line(self):
        return self._json_line

    @json_line.setter 
    def json_line(self, value):
        self._json_line = value

    def get_post_length(self, phase_id):
        if not self.post_length_cache.get(phase_id):
            try:
                post_length = 0
                for post in self.record_processor.get_phase_model().posts:
                    if post["side"] == self.record_processor.get_side():
                        post_length = post["post_length"]
                        break
                self.post_length_cache[phase_id] = post_length
            except Exception as error:
                raise ValueError(f"Error while getting posts from phse_id. phase_id: {phase_id}. Details: {error}")
        return self.post_length_cache.get(phase_id)

    def get_post_number(self):
        post_length = self.get_post_length(self.record_processor.get_phase_id())
        if post_length == 0:
            return 0
        return self.get_distance_cm() // post_length + 1

    @abstractmethod
    def get_capture_timestamp(self) -> int:
        """Capture Timestamp

        Mandatory:
            Yes
        
        Examples:
            # Simple case:
            return int(self.json_line["time"])

            # complex case
            def set_capture_timestamp(self, json_line):
                if (json_line.get('id', None) == 'samplingTime') and json_line.get('param', None) == 'unix':
                    self._capture_timestamp = json_line.get('value', None)            

            def get_capture_timestamp(self):
                return int(self._capture_timestamp)

            # very complex case
            def get_capture_timestamp(self):
                return int(self.get_new_start(
                    idx=self.json_line["index"],
                    end_distance=self.json_line["endDistance"],
                    start_distance=self.json_line["startDistance"],
                    end_time=self.json_line["endTime"],
                    start_time=self.json_line["startTime"],
                    max_distance=self.MAX_DISTANCE))


        Observation:
        You should implement the logic to get the right value.

        Attributes/Properties/Methods available:
            self.json_line[""]
            self.record_processor.method_or_property()
            self.record_processor.stream_message.property()

        """

    def get_capture_local_datetime(self) -> str:
        if self.record_processor.get_farm_id() == UNDEFINED:
            return datetime.utcfromtimestamp(self.get_capture_timestamp()).strftime('%Y-%m-%d %H:%M:%S')
        utc_timestamp = datetime.utcfromtimestamp(self.get_capture_timestamp()).strftime('%Y-%m-%d-%H%M%S')
        return self.record_processor.convert_to_farm_timezone(utc_timestamp)

    def get_capture_local_date(self) -> str:
        return self.get_capture_local_datetime()[:10]

    @abstractmethod
    def get_distance_cm(self) -> int:
        """Row Location / Distance in cm"""

    @abstractmethod
    def get_velocity(self) -> float:
        """Row Location / Velocity"""

    def get_row_session_id(self) -> str:
        return self.record_processor.get_row_session_id()

    @abstractmethod
    def get_height_cm(self) -> int:
        """Row Location / Height in cm"""

    @abstractmethod
    def get_direction(self) -> str:
        """Row Location / Direction"""
        return self.record_processor.get_direction()

    def get_record_type(self):
        return self.record_processor.get_record_type()

    def get_row_session_id(self):
        return self.record_processor.get_row_session_id()

    @abstractmethod
    def get_extra_content(self) -> dict:
        """Extra attributes"""

    def get_cartesian_location(self):
        return self.record_processor.get_cartesian_location(
            distance_cm=self.get_distance_cm(),
            height_cm=self.get_height_cm())



    @abstractmethod
    def get_json_schema(self) -> dict:
        """Return the dict of json schema based on https://json-schema.org/

        Mandatory:
            yes

        It's used to validate the output before put it in the stream.

        You can import it from a different file.
        
        Return:
            dict: Output schema

        Example:
            import plugins.template.schema

            return schema.schema
        """


    def get_base_content(self):
        return { 
            "customer_id": self.record_processor.get_customer_id(),
            "farm_id": self.record_processor.get_farm_id(),
            "phase_id": self.record_processor.get_phase_id(),
            "capture_timestamp": cast_to_int(self.get_capture_timestamp()),
            "capture_local_datetime": self.get_capture_local_datetime(),
            "upload_timestamp": cast_to_int(self.record_processor.get_upload_timestamp()),
            "row_session_id": self.get_row_session_id(),
            "record_type": self.get_record_type(),
            "row_location": {
                "tag_id": self.record_processor.get_tag_id(),
                "row_number": cast_to_int(self.record_processor.get_row_number()),
                "distance_cm": self.get_distance_cm(),
                "height_cm": self.get_height_cm(),
                "post_number": self.get_post_number(),
                "side": self.record_processor.get_side(),
                "direction": self.get_direction(),
                "velocity": self.get_velocity()
            },
            "cartesian_location": self.get_cartesian_location(),
            "crops": self.record_processor.get_crops(),
            "machine_id": self.record_processor.get_machine_id(),
            "hardware_version": self.record_processor.get_hardware_version(),
            "firmware_version": self.record_processor.get_firmware_version(),
            "version": self.VERSION,
            "capture_local_date": self.get_capture_local_date()
        }

    @staticmethod
    def json_validate(instance, schema):
        try:
            schema(instance)
        except Exception as error:
            logger.error(f"Error while validating JSON. Details: {error}")
            return False
        return True

    def get_content(self, json_line):
        """Method to create the json content for data lake.
        
        Returns:
            dict: Instance of data lake record.
        """
        try:
            if not json_line:
                raise Exception(f"Invalid input. Class: {self.__class__.__name__}. Value: {str(json_line)}")
            self.json_line = json_line
            payload = self.get_base_content()
            extra = self.get_extra_content()
            payload.update(extra)
            if self.json_validate(instance=payload, schema=self.get_json_schema()):
                return payload
        except Exception as error:
            raise ValueError("Error while creating message output content. Details: %s" % error)
        # return {
        #     "version" : self.record_processor.get_version(),
        #     "source": self.record_processor.get_source(),
        #     "valid": self.is_valid,
        #     "incoming_timestamp": self.record_processor.get_incoming_timestamp(),
        #     "outgoing_timestamp": self.record_processor.get_outgoing_timestamp(),
        #     "payload" : payload,
        #     "type": self.record_processor.get_record_type(),
        #     "filename": self.record_processor.get_filename()
        # }

