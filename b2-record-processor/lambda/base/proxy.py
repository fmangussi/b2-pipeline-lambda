# -*- coding: utf-8 -*-
# Ecoation RecordProcessorProxy
# 
# Created by: Farzad Khandan (farzadkhandan@ecoation.com)
#

from base.cloud_provider import CloudProviderBase
from helpers import get_logger
from models.record_processor import RecordProcessorBase

logger = get_logger(__name__)

PROCESSOR_PLUGINS_FOLDER = "../plugins/"
PROCESSOR_PREFIX = "raw_processor_"


class RecordProcessorProxy:
    """
    A Proxy class to handle multiple incoming data sources. 
    This is implemented as a singleton, so only one instance is allowed.
    Usage:
       RecordProcessorProxyObj =  RecordProcessorProxy.get_instance(cloud_provider_class)
    """
    _processors = {}
    _cloud_provider = None

    @classmethod
    def register_cloud_provider(cls, cloud_provider):
        if not issubclass(cloud_provider.__class__, CloudProviderBase):
            raise Exception("Wrong cloud provider class. The cloud provider class must be a descendent of CloudProviderBase.")
        cls._cloud_provider = cloud_provider

    def __init__(self, cloud_provider):
        raise Exception("ERROR: Trying to get new instance for RecordProcessorProxy singleton.")

    # Register a Raw Processor
    # Parameters:
    #   data_type: A string representation of the data type and the kind of process, 
    #       must be unique in a project.
    #   processor_class: A class of type RecordProcessorBase
    @classmethod
    def register_processor(cls, data_type, processor):
        if not issubclass(processor, RecordProcessorBase):
            raise Exception("Wrong processor class. The processor class must be a descendent of RecordProcessorBase.")        
        if not issubclass(type(data_type), str):
            raise Exception(f"Wrong data type, expected <string>, provided {str(data_type)}")        
        if data_type in cls._processors:
            raise Exception(f"Duplicate processor type: <{data_type}> already exists.")
        cls._processors[data_type] = processor

    # Automatically registers all the processor plugins
    # Looks in <project_root>/plugins to find new processors
    #
    # Instructions on adding plugins:
    #   1- Create a file named raw_processor_xxx.py
    #   2- Import RecordProcessorBase (from base.raw_processor import RecordProcessorBase)
    #   3- Extend the RecordProcessorBase class and implement the actual raw processor.
    #   4- Create a function in the file to register the raw_processor:
    #       def register(cloud_provider):
    #           return {
    #               'name': <raw_processor_name, string>,
    #               'processor': <an instance of raw_processor_class, a RecordProcessorBase descendent>
    #           }
    #   cloud_provider: an instance of CloudProviderBase class.
    # def auto_register(self):
    #     ins_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), PROCESSOR_PLUGINS_FOLDER)
    #     plugins = os.listdir(plugins_folder)
    #     for filename in plugins:
    #         if filename.startswith(PROCESSOR_PREFIX) and filename.endswith(".py"):
    #             module_name = 'plugins.'+filename
    #             module = __import__(module_name)
    #             register_func = getattr(module, 'register')
    #             try:
    #                 result = register_func(self._cloud_provider)
    #                 self.register_processor(result['name'], result['processor'])
    #             except Exception as ex:
    #                 traceback.print_tb(ex.__traceback__)
    #                 logger.error("Error importing plugin: %s - %s", filename, ex)
    
    # Utility function to check the output returned by the processor
    @staticmethod
    def check_process_output(output):
        try:
            if 'processed' not in output or \
                'continue' not in output or \
                'errors' not in output or \
                'messages' not in output \
            :
                return False
        except:
            return False
        return True

    # Create a logger based on the giver name
    @staticmethod
    def get_logger(name):
        return get_logger(name)

    # Process event main logic
    @classmethod
    def process(cls, event):
        def format_message(messages):
            return f"Record Processor: [{record_processor_name}] - {messages}"

        error_count = 0
        logger.debug("Processing event: %s", event)

        for record_processor_name, record_processor_class in cls._processors.items():
            result = {}
            try:
                #logger.info(format_message("Starting the record processor."))
                result = record_processor_class(record_processor_name, cls._cloud_provider).process(event)
            except Exception as ex:
                logger.error(format_message(f"Error launching event processor."))
                logger.exception(ex)

            if result.get('processed', None):
                logger.info(format_message(f"Result: {result}"))

            if not cls.check_process_output(result):
                logger.error(format_message(f"Processor returned an invalid output: {str(result)}"))
                continue

            for msg in result['messages']:
                logger.debug(msg)

            if result['errors']:
                error_count += len(result['errors'])
                #logger.error(format_message(f"Error processing event: [{event}]"))
                #for err in result['errors']:
                #    logger.error(err)

            if result['processed']:
                logger.info(format_message(f"Event processed { 'with errors' if result['errors'] else 'successfully'}."))

                if not result['continue']:
                    continue

        return error_count
