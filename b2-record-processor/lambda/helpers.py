import traceback

# from base.log_handler import ElasticSearchHandler
from models import b_log

UNDEFINED = '<undefined>'
SYSTEM_LOGGER = b_log.BackendLoggerWrapper()
# ELASTIC_SEARCH_HANDLER = ElasticSearchHandler()
# ELASTIC_SEARCH_HANDLER.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))


# Create a logger based on the giver name
def get_logger(name=""):
    return b_log.BackendLoggerWrapper()


def format_log_message(method, arguments, message):
    stack = traceback.format_exc()
    return f"{message} \rMETHOD: [{method}] \rARGS: \r{arguments} \rTRACEBACK: \r{stack}"


def system_log_error(name, method, arguments, message):
    if not name:
        name = __name__
    logger = get_logger(name)
    message = format_log_message(method, arguments, message)
    logger.error(message)


def system_log_debug(name, method, arguments, message):
    if not name:
        name = __name__
    logger = get_logger(name)
    message = format_log_message(method, arguments, message)
    logger.debug(message)


def system_log_exception(exception):
    logger = get_logger()
    logger.exception(exception)


def system_log_info(name, message):
    if not name:
        name = __name__
    logger = get_logger(name)
    logger.info(message)


def system_log_warning(name, message):
    if not name:
        name = __name__
    logger = get_logger(name)
    logger.warning(message)

def json_validate(instance, schema):
    try:
        # jsonschema.validate(instance=instance, schema=schema)
        schema(instance)
    except Exception as error:
        # traceback.print_tb(error.__traceback__)
        # logger = get_logger(__name__)
        SYSTEM_LOGGER.error("Error while validating JSON.")
        raise


def cast_to_int(value):
    return int(float(str(value)))
