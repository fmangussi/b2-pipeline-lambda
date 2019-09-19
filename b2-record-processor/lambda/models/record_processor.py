# -*- coding: utf-8 -*-
# Ecoation RawProcessorBase
#
# Created by: Farzad Khandan (farzadkhandan@ecoation.com)
# Modified by: Fernando Mangussi <fernando@ecoation.com>
#

import inspect
import json
import os
import re
import traceback
import uuid
from abc import abstractmethod
from datetime import datetime, date
from typing import Optional

import dateutil.parser as parser
from dateutil import tz

import datamodule.farm_model as farm
import datamodule.machine_model as machine_model
import datamodule.phase_model as phase
from base.processor import ProcessorBase
from helpers import (
    system_log_error,
    system_log_debug,
    system_log_info,
    format_log_message,
    system_log_exception,
    UNDEFINED,
    cast_to_int, system_log_warning)
from models import b_log
from models.event_message import (
    StreamMessageExtraContent,
    StreamEventMessage,
    InvalidStreamMessage,
    RecordProcessStreamMessage)
from models.stage_file import StageCSVReaderFile, StageCSVWriterFile

BASE_NAME_V1 = r"/(?P<tag_id>[^-]+)-(?P<timestamp>[^-]+)-(?P<extra_info>[^-]+)-(?P<identifier>.{6})\.(?P<fileextension>.*$)"
BASE_NAME_V2 = r'/(?P<tag_id>[^-]+)-(?P<rsid>[^-]+)-(?P<timestamp>[^-]+)-(?P<extra_info>[^-]+)-(?P<identifier>.{6})\.(?P<fileextension>.*$)'
RE_BASE_NAME_V1 = re.compile(BASE_NAME_V1)
RE_BASE_NAME_V2 = re.compile(BASE_NAME_V2)
RE_KEY_FILENAME_V1 = re.compile(
    r"^.*/(?P<file_type>[^/]+)/(?P<machine_id>[^/]+)/(?P<upload_date>.{4}-.{2}-.{2})" + BASE_NAME_V1)
RE_KEY_FILENAME_V2 = re.compile(
    r'^.*/(?P<file_type>[^/]+)/(?P<machine_id>[^/]+)/(?P<upload_date>.{4}-.{2}-.{2})' + BASE_NAME_V2)
RE_KEY_FILENAME_V3 = re.compile(r'^.*/(?P<file_type>[\w-]+)' + BASE_NAME_V2)
RE_KEY_FILENAME_MODELS_V1 = re.compile(
    r'/(?P<file_type>[^/]+)/(?P<phase_id>[^/]+)/(?P<row_number>[^/]+)/(?P<rsid>[^/]+)/[^.]+\.(?P<fileextension>.*$)')


class RecordProcessorBase(ProcessorBase):
    """Record processor class

    It implement the logic to create the output for the data lake.
    """

    post_length = {}
    # common attributes
    cartesian_location: property
    crops: property
    customer_id: property
    farm_id: property
    filename: property
    firmware_version: property
    hardware_version: property
    height_cm: property
    incoming_timestamp: property
    machine_id: property
    outgoing_timestamp: property
    phase_id: property
    post_number: property
    record_type: property
    row_number: property
    side: property
    source: property
    tag_id: property
    upload_timestamp: property
    version: property
    direction: property
    row_session_id: property
    filename_info: property

    # controller attributes
    debug_messages: property
    error_messages: property
    event_message: property
    stream_message: property

    # invalid_output = property(get_invalid_output)
    # outputs = property(get_outputs)
    processed_status: property

    # Initialization
    # Parameters:
    #   name: A generic name for the processor
    #   cloud_provider: an instance of RawCloudProvider class to interact with the underlying cloud service
    def __init__(self, name, cloud_provider):
        super().__init__(name, cloud_provider)
        self.stage_files_processed_count = 0
        self.stage_files_total = 0
        self.input_lines_rejected_count = 0
        self.input_lines_tbd_rejected_count = 0
        self.stage_files_rejected_count = 0
        self.output_lines_created = 0
        self._error_messages = []
        self.update_properties()
        self._OB_PHASE = phase.PhaseModel()
        self.OB_FARM = farm.FarmModel()
        self.OB_MACHINE = machine_model.MachineModel()
        self._machine = None
        # self._cartesian_location = None
        self._crops = None
        self._customer_id = None
        self._debug_messages = []
        self._error_messages = []
        self._event_message: Optional[StreamEventMessage] = None
        self._farm_id = None

        self._outgoing_timestamp = None
        self._phase_id = None
        self._processed_status = False
        self._row_number = None
        self._row_session_id = None
        self._side = None
        self._stream_message = None
        self._upload_timestamp = None

        self._is_event_invalid = False
        self._invalid_event_content = None
        self._stage_file = None
        self._row = None
        self._farm_zone = None
        self._timestamp_conversion_cache = None
        self._incoming_timestamp = None
        self._current_payload = None
        self._file_match = None

        self._stage_files = []

        self._uuid_hash = str(uuid.uuid1())

    def get_phase_model(self):
        self.get_row()
        return self._OB_PHASE

    @classmethod
    def update_properties(cls):
        cls.crops = property(cls.get_crops)
        cls.customer_id = property(cls.get_customer_id)
        cls.farm_id = property(cls.get_farm_id)
        cls.filename = property(cls.get_filename)
        cls.firmware_version = property(cls.get_firmware_version)
        cls.hardware_version = property(cls.get_hardware_version)
        cls.height_cm = property(cls.get_height_cm)
        cls.incoming_timestamp = property(cls.get_incoming_timestamp)
        cls.machine_id = property(cls.get_machine_id)
        cls.outgoing_timestamp = property(cls.get_outgoing_timestamp)
        cls.phase_id = property(cls.get_phase_id)
        cls.record_type = property(cls.get_record_type)
        cls.row_number = property(cls.get_row_number)
        cls.side = property(cls.get_side)
        cls.source = property(cls.get_source)
        cls.tag_id = property(cls.get_tag_id)
        cls.upload_timestamp = property(cls.get_upload_timestamp)
        cls.version = property(cls.get_version)
        cls.direction = property(cls.get_direction)
        cls.row_session_id = property(cls.get_row_session_id)
        cls.filename_info = property(cls.get_filename_info)
        cls.debug_messages = property(cls.get_debug_messages)
        cls.error_messages = property(cls.get_error_messages, cls.set_error_messages)
        cls.event_message = property(cls.get_event_message)
        cls.stream_message = property(cls.get_stream_message)
        cls.processed_status = property(cls.get_processed_status, cls.set_processed_status)

    def get_log_options(self):
        log_options = self.get_filename_info().copy()
        log_options["file"] = os.path.basename(self.stream_message.filename)
        try:
            phase_id = log_options.get("phase_id", None) or self.get_phase_id()
        except:
            phase_id = ""
        try:
            customer_id = log_options.get("customer_id", None) or self.get_customer_id()
        except:
            customer_id = ""

        log_options["phase_id"] = phase_id or UNDEFINED
        log_options["customer_id"] = customer_id or UNDEFINED
        keys_to_be_removed = ["identifier", "fileextension", "timestamp", "extra_info"]
        for key in log_options:
            if log_options[key] == UNDEFINED:
                keys_to_be_removed.append(key)
        for key in keys_to_be_removed:
            if key in log_options:
                del log_options[key]
        return log_options

    @staticmethod
    def extract_info_from_filename(filename):
        # if not self._file_match:
        file_to_match = f'/{filename}'
        file_match = (
                RE_KEY_FILENAME_V2.match(file_to_match)
                or
                RE_KEY_FILENAME_V1.match(file_to_match)
                or
                RE_KEY_FILENAME_V3.match(file_to_match)
                or
                RE_KEY_FILENAME_MODELS_V1.match(file_to_match)
                or
                RE_BASE_NAME_V1.match(file_to_match)
                or
                RE_BASE_NAME_V2.match(file_to_match)
        )
        file_match_groups = file_match.groupdict() if file_match else {}

        result = dict(
            file_type=UNDEFINED,
            machine_id=UNDEFINED,
            upload_date=UNDEFINED,
            tag_id=UNDEFINED,
            rsid=UNDEFINED,
            timestamp=UNDEFINED,
            extra_info=UNDEFINED,
            identifier=UNDEFINED,
            fileextension=UNDEFINED,
            phase_id=UNDEFINED,
        )
        result.update(file_match_groups)
        return result

    def get_filename_info(self):
        result = self.extract_info_from_filename(self.stream_message.filename)
        self._file_match = result
        return result

    @property
    def current_payload(self):
        return self._current_payload

    @current_payload.setter
    def current_payload(self, value):
        self._current_payload = value

    @staticmethod
    def open_stage_file(filepath) -> StageCSVReaderFile:
        try:
            return StageCSVReaderFile(filepath=filepath)
        except Exception as error:
            raise ValueError("Error while opening the stage file. Details %s" % error)

    @property
    def name(self):
        return self._name

    @property
    def cloud_provider(self):
        return self._cloud_provider

    @staticmethod
    @abstractmethod
    def types_to_be_processed() -> list:
        """List of type of files that will be processed by the record processor.

        Returns:
            list: list of file types. e.g.: wave|video|image|aux|label

        Example:
            return ["<type_of_file>"]
        """

    def log_error(self, method, arguments, error_message):
        try:
            error_message = f"{self.__class__.__name__} - {error_message}"
            fmt_message = format_log_message(method, arguments, error_message)
            self._error_messages.append(fmt_message)
            system_log_error(
                name=self.name,
                method=method,
                arguments=arguments,
                message=error_message)
        except Exception as error:
            print(f"Original error: [{error_message}], log error: [{str(error)}]")
            traceback.print_tb(error.__traceback__)

    def log_debug(self, method, arguments, message):
        message = f"{self.__class__.__name__} - {message}"
        fmt_message = format_log_message(method, arguments, message)
        self._debug_messages.append(fmt_message)
        system_log_debug(
            name=self.name,
            method=method,
            arguments=arguments,
            message=message)

    def log_info(self, message):
        message = f"{self.__class__.__name__} - {message}"
        self._debug_messages.append(message)
        system_log_info(name=self.name, message=message)

    def log_warning(self, message):
        message = f"{self.__class__.__name__} - {message}"
        system_log_warning(name=self.name, message=message)

    def get_continue(self):
        if self.processed_status:
            return False
        return True

    def get_stats(self):

        # Return the result
        return {
            # If processed, this flag must be set to true
            'processed': self.processed_status,
            # If other processors can also process this event, this flag must be set to true,
            #   if this is the only (and final) processor, set it to false
            'continue': self.get_continue(),
            # Return all the error messages here, the Proxy will log them
            'errors': self.error_messages,
            # Return all the DEBUG messages here, the Proxy will log them
            'messages': [],  # self.debug_messages
            'output_lines_created': self.output_lines_created,
            'input_lines_rejected': self.input_lines_rejected_count,
            'input_lines_tbd_rejected': self.input_lines_tbd_rejected_count,
            'stage_files_total': self.stage_files_total,
            'stage_files_processed': self.stage_files_processed_count,
            'stage_files_rejected': self.stage_files_rejected_count
        }

    def clean_stats(self):
        self._is_event_invalid = False
        self._error_messages = []
        self._debug_messages = []
        self._stage_files = []
        # self._outputs = []
        # self._invalid_output = []
        self._customer_id = None
        self._farm_id = None
        self._phase_id = None
        self._upload_timestamp = None
        # self._cartesian_location = None
        self._crops = None
        self._side = None
        self._row_number = None
        self._outgoing_timestamp = None
        self._farm_zone = None

    def invalidate_event(self, event, error, stack):
        self._is_event_invalid = True
        if self.stream_message:
            self.stream_message.set_invalid()
            self._invalid_event_content = InvalidStreamMessage.from_stream_message(
                stream_message=self.stream_message,
                reason=str(error),
                invalid_source=self.__class__.__name__,
                stack=stack).get_json(self.stream_message.list_of_files)
        else:
            payload = {
                "record_processor": self.__class__.__name__,
                "invalid_event": event,
                "stack": stack
            }

            invalid_event = InvalidStreamMessage()
            invalid_event.source = ""
            invalid_event.incoming_timestamp = datetime.now(),
            invalid_event.outgoing_timestamp = datetime.now(),
            # invalid_event.payload = payload,
            invalid_event.type = "event",
            invalid_event.filename = "",
            invalid_event.reason = str(error),
            invalid_event.invalid_source = "record_processor"

            return invalid_event.get_json(payload)

    def get_invalid_event(self):
        return self._invalid_event_content

    @staticmethod
    def invalidate_record(record):
        record.invalidate()
        # self.invalid_output.append((record, record.get_content()))

    def get_stage_file(self):
        if not self._stage_file:
            self._stage_file = StageCSVWriterFile(filename=os.path.basename(self.stream_message.filename))
        return self._stage_file

    @property
    def uuid_hash(self):
        return self._uuid_hash

    def _get_stage_destination_path(self, processor_name):
        return f'''{processor_name}/process_date={date.today().isoformat()}/datatype={self.get_record_type()}/{os.path.basename(self.stream_message.filename)}__{uuid.uuid1()}.csv'''

    def close_stage_file(self, stage_writer: StageCSVWriterFile):
        try:
            destination_path = self._get_stage_destination_path('record-processor')
            cloud_filepath = self.cloud_provider.format_stage_filename(destination_path)
            self.log_info(f"uploading the stage file {cloud_filepath}.")
            # print("stage_writer:", stage_writer)
            if stage_writer.emptyfile:
                self.log_info(f"stage file is empty.")
                return
            stage_writer.close()
            self._cloud_provider.upload_stage_content(data_frame=stage_writer.data_frame,
                                                      destination_path=cloud_filepath)
            self._stage_files.append(destination_path)
        except Exception as error:
            traceback.print_tb(error.__traceback__)
            raise
        return destination_path

    def write_to_stage(self, output):
        if output:
            stage_writer = self.get_stage_file()
            stage_writer.write(output)

    @abstractmethod
    def create_message_payload(self, payload) -> iter:
        """Logic to create output payload.

        Mandatory:
            Yes

        Returns:
            iter: iterate over the output

        The input is a message comming from Kinesis that is available in the variable

        self.stream_message: base.eventmessage.StreamMessage

        This object represent all the messages exchaged throught Kinesis.

        Some properties are:
            version
            source
            valid
            incoming
            outgoing
            payload
            type
            filename

        Please see the class description to understand all the properties.

        Example of implementation:
        record = AuxDatalakeRecord(record_processor=self)
        yield record.get_content(payload)
        """

    def send_to_processed_data_stream(self):
        if self._is_event_invalid:
            return
        if not self._stage_files:
            self.log_info("There is not stage files to be processed.")
            return

        if self.output_lines_created <= 0:
            return
        try:
            extra = StreamMessageExtraContent()
            extra.set_payload(self._stage_files)
            extra.set_customer_id(self.get_customer_id())
            extra.set_farm_id(self.get_farm_id())
            extra.set_phase_id(self.get_phase_id())
            extra.set_row_session_id(self.get_row_session_id())
            extra.set_machine_id(self.get_machine_id())
            extra.set_hardware_version(self.get_hardware_version())
            extra.set_firmware_version(self.get_firmware_version())
            extra.set_tag_id(self.get_tag_id())
            extra.set_row_number(self.get_row_number())
            extra.set_post_number(0)
            extra.set_side(self.get_side())
            extra.set_direction(self.get_direction())
            extra.set_outgoing_timestamp(self.get_outgoing_timestamp())
            output_message = self.stream_message.get_content_output(extra)
            self.cloud_provider.put_in_processed_data_stream(payload=output_message)
            self.log_info(f"output_event: {json.dumps(output_message)}")
        except Exception as error:
            traceback.print_tb(error.__traceback__)
            self.log_error(
                method=inspect.stack()[0][3],
                arguments=locals(),
                error_message=f"Error while sending to processed data stream. Details: {str(error)}")
            raise

    def put_invalid_data_stream(self, content):
        try:
            # self.log_info(f"put_invalid_data_stream")
            self.cloud_provider.put_in_invalid_data_stream(payload=content)
            self.log_info(f"send_to_invalid_data_stream: 1\n{json.dumps(content)}")
        except Exception as error:
            traceback.print_tb(error.__traceback__)
            self.log_error(
                method=inspect.stack()[0][3],
                arguments=locals(),
                error_message=f"CRITICAL ERROR: Error while sending to invalid data stream. Details: {str(error)}. Content: {content}")

    def send_to_invalid_data_stream(self):
        if self._is_event_invalid:
            invalid_content = self.get_invalid_event()
            self.put_invalid_data_stream(content=invalid_content)
            self.log_info(f"Invalid Event 1: {str(invalid_content)}")
            return

        # for record, content in self.invalid_output:
        #     self.put_invalid_data_stream(content)

    def variable_initialization(self) -> None:
        """Variable Initialization

        It's called by the process() method
        """
        self._error_messages = []
        self._debug_messages = []
        # self._outputs = []
        # self._invalid_output = []

    def update_stats(self):
        pass

    def on_line_output_exception(self, input_line: dict, payload_count: int, error: Exception):
        """
        This method is called every time an error is raised during the create_output_payload.
        :param input_line: dict
        :param payload_count: int
        :param error: Exception
        :return: None
        """
        if self.get_tag_id() == "TBD":
            self.input_lines_tbd_rejected_count += 1
            return
        self.input_lines_rejected_count += 1
        self.log_info(
            f"Error while creating the output payload. Details: input_line:[{str(input_line)}] error:[{str(error)}]")

    def create_output_payload(self, input_stage_file: StageCSVReaderFile, filepath):
        input_lines_count = 0
        output_lines_count = 0
        tag_id_list = set()
        input_line: dict
        for input_line in input_stage_file:
            try:
                self.current_payload = input_line
                tag_id_list.add(self.get_tag_id())
                input_lines_count += 1
                if input_lines_count % 10000 == 0:
                    self.log_info(f"Processing {self.get_record_type()} content: {input_lines_count} processed.")
                for output in self.create_message_payload(input_line):
                    output_lines_count += 1
                    self.write_to_stage(output)
            except Exception as error:
                self.on_line_output_exception(input_line, input_lines_count, error)
        self.log_info(f"Processing {self.get_record_type()} content: {input_lines_count} processed.")

        if "TBD" in tag_id_list:
            tag_id_list.remove("TBD")
            self.log_info(f"Tag ID TBD was found in the stage file. Details: filepath:{filepath} ")

        if len(tag_id_list) > 1:
            self.log_warning(f"More than one Tag ID was found in the stage file. Details: filepath:{filepath}")

        filename_tag_id = self.extract_info_from_filename(os.path.basename(input_stage_file.filepath))['tag_id']
        for tag_id in tag_id_list:
            if tag_id != filename_tag_id:
                self.log_warning(
                    f"The Tag ID <<{tag_id}>> didn't match with the same Tag ID in the filename <<{filename_tag_id}>>. Details: filepath:{filepath}")
        return output_lines_count

    def process(self, event):
        """
            Process a Raw event
            Parameters:
              event: a typical Lambda Event record
            Returns:
              {
                  'processed': is the event processed?,
                  'continue': should the event hand over to other processors?,
                  'errors': a list of erros strings,
                  'messages': a list of messages
              }
        :param event: dict
        :return: dict
        """
        try:
            self.clean_stats()
            self.set_processed_status(False)
            self.set_event_message(event)
            if not self.event_message:
                raise ValueError("Event Message invalid.")
            if self.record_type not in self.types_to_be_processed():
                return
            b_log.blog_info(message=f"Starting {self.stream_message.type}",
                            operation=b_log.OPERATION_PROCESSING_A_FILE_OR_RECORD,
                            status=b_log.STATUS_IT_IS_STARTING,
                            optional_info=self.get_log_options())
            self.set_processed_status(True)
            self.variable_initialization()
            self.process_stage_files()
            self.close_stage_file(self._stage_file)
            self.send_to_processed_data_stream()
        except Exception as error:
            self.error_messages.append(error)
            traceback.print_tb(error.__traceback__)
            b_log.blog_info(message=error,
                            operation=b_log.OPERATION_PROCESSING_A_FILE_OR_RECORD,
                            status=b_log.STATUS_IT_FAILED,
                            optional_info=self.get_log_options())
            try:
                self.invalidate_event(
                    event=event,
                    error=error,
                    stack=traceback.format_tb(error.__traceback__))
                self.send_to_invalid_data_stream()
            except Exception as invalidate_error:
                self.log_error(
                    method=inspect.stack()[0][3],
                    arguments=locals(),
                    error_message=f"CRITICAL ERROR while sending to invalid stream. Details: event: [{event}] error: [{str(invalidate_error)}]")
            self.log_error(
                method=inspect.stack()[0][3],
                arguments=locals(),
                error_message=f"CRITICAL ERROR: Error while process the event. Details: {str(error)}")
        else:
            b_log.blog_info(message="File processed successfully",
                            operation=b_log.OPERATION_PROCESSING_A_FILE_OR_RECORD,
                            status=b_log.STATUS_IT_WAS_SUCCESSFUL,
                            optional_info=self.get_log_options())
            self.data_processed()
            self._error_messages = []
        finally:
            self.update_stats()
            return self.get_stats()

    def data_processed(self):
        if self.output_lines_created <= 0:
            return
        try:
            if not self.get_phase_model().customer_id:
                self.get_phase_model().get_by_id(self.phase_id)
            self.get_phase_model().data_processed()
        except Exception as error:
            self.log_info(f"Error while calling the get_phase_model().get_by_id(). Details: {error}")

    def on_process_stage_files_error(self, filepath, error):
        self.stage_files_rejected_count += 1
        self.log_info(f"Error while processing stage files. Details: {str(error)}. File path: {filepath}")

    def process_stage_files(self):
        self.stage_files_total = len(self.stream_message.list_of_files)
        for filepath in self.stream_message.list_of_files:
            stage_output_lines_created = 0
            try:
                self.log_info(f"Processing {self.stream_message.type} - {filepath}")
                self.get_stage_file()
                input_stage_file = self.open_stage_file(filepath=self.cloud_provider.format_stage_filename(filepath))
                stage_output_lines_created = self.create_output_payload(input_stage_file=input_stage_file, filepath=filepath)
            except Exception as error:
                self.on_process_stage_files_error(filepath, error)
            else:
                self.output_lines_created += stage_output_lines_created
                self.stage_files_processed_count += 1
            if stage_output_lines_created == 0:
                self.log_warning(f"No output was created. Details: [{filepath}]")

    def set_processed_status(self, value):
        self._processed_status = value

    def get_processed_status(self):
        return self._processed_status

    def get_error_messages(self):
        return self._error_messages

    def set_error_messages(self, value):
        self._error_messages = value

    def get_debug_messages(self):
        return self._debug_messages

    def get_event_message(self) -> StreamEventMessage:
        return self._event_message

    def set_event_message(self, value):
        try:
            self._event_message = self.cloud_provider.create_event_message(value)
            self.set_stream_message(self.event_message)
        except:
            self._event_message = None
            raise

    def set_stream_message(self, event_message):
        self._stream_message = RecordProcessStreamMessage(dict=event_message.get_event_data_decoded())

    def get_stream_message(self) -> RecordProcessStreamMessage:
        return self._stream_message

    @abstractmethod
    def get_tag_id(self) -> str:
        """Returns the Tag ID

        Mandatory:
            Yes
        
        Returns:
            string: tag id

        Example:
            return self.stream_message.payload["location"]["tagid"]

            or

            return self._tag_id

        Each file has the own logic to get the properties.
        """
        return "<tag_id_value>"

    def get_farm_zone(self):
        if not self._farm_zone:
            self._farm_zone = self.OB_FARM.get_by_farm_id(self.farm_id).timezone
            self._farm_zone = tz.gettz(self._farm_zone)
        return self._farm_zone

    def convert_to_farm_timezone(self, time_utc):
        def calculate(time_utc):
            to_zone = self.get_farm_zone()
            from_zone = tz.gettz('UTC')
            utc = datetime.strptime(time_utc, '%Y-%m-%d-%H%M%S')
            utc = utc.replace(tzinfo=from_zone)
            to_time = utc.astimezone(to_zone)
            text = str(to_time)
            date = parser.parse(text)
            iso_format = date.strftime("%Y-%m-%d %H:%M:%S")
            return iso_format

        def get_cache_value(time_utc):
            if not self._timestamp_conversion_cache:
                self._timestamp_conversion_cache = {}
            result = self._timestamp_conversion_cache.get(time_utc)
            if not result:
                result = calculate(time_utc)
                self._timestamp_conversion_cache[time_utc] = result
            return result

        return get_cache_value(time_utc)

    def get_row(self):
        if not self._row:
            # if not self.tag_id.upper() == "TBD":
            try:
                self._row = self._OB_PHASE.find_row(self.tag_id, load_phase=True)
                if not self._row:
                    raise Exception(f"Tag ID [{self.tag_id}] was not found.")
            except Exception as error:
                system_log_exception(error)
                raise
        return self._row

    def get_customer_id(self):
        # print("before: self._customer_id: ", str(self._customer_id))
        if not self._customer_id:
            if self.get_tag_id().upper() == "TBD":
                machine_id = self.get_machine_id()
                try:
                    self._customer_id = self.OB_MACHINE.get_by_id(machine_id=machine_id).customer_id
                except Exception as error:
                    self.log_error(
                        method=inspect.stack()[0][3],
                        arguments=locals(),
                        error_message=f"Error while getting machine ID Details: machine_id={machine_id}, error={str(error)}")
                    self._customer_id = UNDEFINED
            else:
                self._customer_id = (self.get_row()['customer_id'])
            # print("returning self._customer_id: ", self._customer_id)
        # print("after: self._customer_id: ", str(self._customer_id))
        return self._customer_id

    def get_farm_id(self):
        if not self._farm_id:
            self._farm_id = self.get_row()["farm_id"]
        return self._farm_id

    def get_phase_id(self):
        if not self._phase_id:
            self._phase_id = self.get_row()["phase_id"]
        return self._phase_id

    def get_crops(self):
        if not self._crops:
            self._crops = self.get_row()["crops"]
            # self._crops = json.dumps(self._crops)
        return self._crops

    def get_machine_id(self):
        return self.filename_info['machine_id']

    def get_hardware_version(self):
        return "0.1"

    def get_firmware_version(self):
        return "0.1"

    def get_record_type(self):
        if self.stream_message:
            return self.stream_message.type

    def get_filename(self):
        return self.stream_message.filename

    def get_incoming_timestamp(self):
        if not self._incoming_timestamp:
            # self._incoming_timestamp = str((parser.parse(self.stream_message.incoming_timestamp.split(".")[0])).replace(microsecond=0))
            self._incoming_timestamp = self.stream_message.incoming_timestamp[:19]
        return self._incoming_timestamp

    def get_source(self):
        return self.stream_message.source

    def get_side(self):
        if not self._side:
            self._side = self.get_row()["side"]
        return self._side

    def get_row_number(self):
        if not self._row_number:
            self._row_number = cast_to_int(self.get_row()["row_number"])
        return self._row_number

    def get_post_length(self, phase_id):
        if not self.post_length.get(phase_id):
            self.post_length[phase_id] = self.get_phase_model().get_by_id(phase_id).posts[0]['post_length_cache']
        return self.post_length.get(phase_id)

    def get_height_cm(self):
        return 0

    def get_upload_timestamp(self):
        if not self._upload_timestamp:
            self._upload_timestamp = cast_to_int(
                datetime.strptime(self.stream_message.incoming_timestamp, "%Y-%m-%d %H:%M:%S").timestamp())
        return self._upload_timestamp

    def get_outgoing_timestamp(self):
        if not self._outgoing_timestamp:
            self._outgoing_timestamp = str(datetime.utcnow().replace(microsecond=0))
        return self._outgoing_timestamp

    @staticmethod
    def get_version():
        return 1.0

    def get_direction(self):
        return UNDEFINED

    def set_row_session_id(self, value):
        self._row_session_id = value

    def get_row_session_id(self):
        payload = self.current_payload if self.current_payload else {}
        location_rsid = payload.get("location", {}).get("rsid", None)
        root_rsid = payload.get("rsid", None)
        return self._row_session_id or location_rsid or root_rsid or UNDEFINED

    def get_cartesian_location(self, distance_cm, height_cm):
        default = {'x': 0, 'y': 0, 'z': 0}
        try:
            if self.get_phase_id() == UNDEFINED:
                return default
            return self.get_phase_model().get_cartesian_location(
                row_number=self.row_number,
                side=self.side,
                distance_cm=distance_cm,
                height_cm=height_cm)
        except Exception as error:
            traceback.print_tb(error.__traceback__)
            self.log_error(
                method=inspect.stack()[0][3],
                arguments=locals(),
                error_message=f"Error while getting the cartesian location.. Details: {str(error)}")
            raise
