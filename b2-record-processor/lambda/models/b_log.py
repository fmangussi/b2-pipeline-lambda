####################################################################
# Standard Logging

import os
import logging
# from config import APP_LOGGER_PROCESS

APP_LOGGER_PROCESS = 'BCAPI'
FORMAT = "%(message)s"
APP_LOG_LEVEL = os.getenv("LOG_LEVEL", logging.INFO)
APP_LOGGER = None

# constants:
# PROCESS: A process identifier representing the process which creates the log. can be one of:
PROCESS_RAW_PROCESSOR = "RAWPROC"
PROCESS_RECORD_PROCESSOR = "RECPROC"
PROCESS_S3_WRITER = "S3W"
PROCESS_FAST_ACCESS_WORKER = "FAW"
PROCESS_BACKEND_UI = "BUI"
PROCESS_BACKEND_SOFTWARE_API = "BSAPI"
PROCESS_BACKEND_CONFIGURATION_API = "BCAPI"
PROCESS_SOFTWARE_DATA_WORKER = "SDW"
PROCESS_SOFTWARE_API = "SAPI"
PROCESS_MODEL_TRIGGER = "MTRG"
PROCESS_ROW_TOMATO_FRUIT_COUNT = "MDL_0001"
PROCESS_TOMATO_STRESS_PREDICTION = "MDL_0002"

# OPERATION: The kind of operation that generates the log. Typical values are:
OPERATION_PROCESSING_A_FILE_OR_RECORD = "PROCESS"
OPERATION_READING_A_FILE_FROM_S3_FILE_SYSTEM = "READ"
OPERATION_WRITING_A_FILE_TO_S3_FILE_SYSTEM = "WRITE"
OPERATION_GET_DATA_FROM_DATABASE = "GET"
OPERATION_STORING_DATA_IN_DATABASE = "STORE"
OPERATION_INVOKIN_STARTING_AN_EXTERNAL_PROCESS_JOB = "JOB"
OPERATION_CALLING_AN_API = "API"
OPERATION_REPORTING_THE_EVENT = "EVENT"

# STATUS: The status of the process. Possible values are:
STATUS_IT_IS_STARTING = "START"
STATUS_IT_WAS_SUCCESSFUL = "SUCCESS"
STATUS_IT_FAILED = "FAILED"
STATUS_IT_CAUSED_AN_ERROR = "ERROR"
STATUS_IT_IS_STILL_IN_PROGRESS = "REPORT"


def init(process_name=APP_LOGGER_PROCESS, log_level=APP_LOG_LEVEL):
    global APP_LOGGER
    global APP_LOGGER_PROCESS
    global FORMAT
    APP_LOGGER_PROCESS = process_name
    APP_LOGGER = logging.getLogger()
    APP_LOGGER.propagate = False
    APP_LOGGER.handlers = []
    FORMAT = f'%(asctime)s - %(levelname)s - {APP_LOGGER_PROCESS} %(message)s'
    logging.basicConfig(level=log_level, format=FORMAT)


def blog_format_message(message, operation, status, optional_info={}):
    # optional_info is of type dict
    extra = f'- {operation} - {status}'
    if optional_info and len(optional_info) > 0:
        opstr = ''
        for k, v in optional_info.items():
            opstr = opstr + f' - {k}:{v}'
        extra += opstr
    msg = f'{extra} : {message}'
    return msg


def blog_info(message, operation, status, optional_info={}):
    if not APP_LOGGER:
        init()
    APP_LOGGER.info(blog_format_message(message, operation, status, optional_info))


def blog_debug(message, operation, status, optional_info={}):
    if not APP_LOGGER:
        init()
    APP_LOGGER.debug(blog_format_message(message, operation, status, optional_info))


def blog_error(message, operation, status, optional_info={}):
    if not APP_LOGGER:
        init()
    APP_LOGGER.error(blog_format_message(message, operation, status, optional_info))


def blog_exception(exception):
    try:
        APP_LOGGER.exception(exception)
    except AttributeError:
        init()
        blog_exception(exception)


def blog_warning(message, operation, status, optional_info={}):
    if not APP_LOGGER:
        init()
    APP_LOGGER.warning(blog_format_message(message, operation, status, optional_info))


class BackendLoggerWrapper:

    @staticmethod
    def info(message, operation=OPERATION_PROCESSING_A_FILE_OR_RECORD, status=STATUS_IT_IS_STILL_IN_PROGRESS, optional_info={}):
        blog_info(message=message, operation=operation, status=status, optional_info=optional_info)

    @staticmethod
    def debug(message, operation=OPERATION_PROCESSING_A_FILE_OR_RECORD, status=STATUS_IT_IS_STILL_IN_PROGRESS, optional_info={}):
        blog_debug(message=message, operation=operation, status=status, optional_info=optional_info)

    @staticmethod
    def error(message, operation=OPERATION_PROCESSING_A_FILE_OR_RECORD, status=STATUS_IT_IS_STILL_IN_PROGRESS, optional_info={}):
        blog_error(message=message, operation=operation, status=status, optional_info=optional_info)

    @staticmethod
    def warning(message, operation=OPERATION_PROCESSING_A_FILE_OR_RECORD, status=STATUS_IT_IS_STILL_IN_PROGRESS, optional_info={}):
        blog_warning(message=message, operation=operation, status=status, optional_info=optional_info)

    @staticmethod
    def exception(exception):
        blog_exception(exception=exception)
