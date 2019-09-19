# -*- coding: utf-8 -*-
# Ecoation CloudProviderBase
#
# Created by: Farzad Khandan (farzadkhandan@ecoation.com)
# Modified by: Fernando Mangussi <fernando@ecoation.com>
#

from abc import ABC, abstractmethod

from helpers import get_logger

logger = get_logger(__name__)


class CloudProviderBase(ABC):
    '''
    This is a generic Cloud provider to add an abstraction layer between business rules and physical implementation
    for the Raw Processors
    '''

    @abstractmethod
    def get_object(self, obj_address, obj_name):
        raise Exception("Not Implemented")  # Abstract Methods! Remove this line when implementing the method

    @abstractmethod
    def download_object(self, obj_address, obj_name, local_file_location):
        raise Exception("Not Implemented")  # Abstract Methods! Remove this line when implementing the method

    @abstractmethod
    def put_object(self, obj_address, obj_name, obj_content):
        raise Exception("Not Implemented")  # Abstract Methods! Remove this line when implementing the method

    @abstractmethod
    def copy_object(self, source_obj_address, source_obj_name, dest_obj_address, dest_obj_name):
        raise Exception("Not Implemented")  # Abstract Methods! Remove this line when implementing the method

    @abstractmethod
    def send_to_stream(self, stream_name, stream_payload):
        raise Exception("Not Implemented")  # Abstract Methods! Remove this line when implementing the method

    @abstractmethod
    def create_event_message(self, event):
        """Create a EventMessage object.

        Args:
            event (String): content of the event

        Returns
            eventmessage.EventMessage object

        event sample content:
            {
              "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "2019-04-25 15:45:32",
                "sequenceNumber": "49594210671678830695451044071707598249733934195629097026",
                "data": "eyJ2ZXJzaW9uIjogMS4wLCAic291cmNlIjogImxhYmVsLzg5NGY5NDQyOGE1NjJi",
                "approximateArrivalTimestamp": 1556207132.798,
                "uncompress-data": {
                  "version": 1.0,
                  "source": "label/894f94428a562ba7f524f40b62802472/2019-04-23/0438763235890056-20190423-left-191134.tar.bz2",
                  "valid": true,
                  "incoming_timestamp": "1970-01-01 00:00:00",
                  "outgoing_timestamp": "2019-04-25 15:45:32",
                  "type": "label",
                  "filename": "0438763235890056-20190423-left-191134.label",
                  "payload": {
                    "startDistance": 124,
                    "seq": "871fc2f1e059298a0aa3fc0d6a366b9e",
                    "tagId": "0438763235890056",
                    "endDistance": 250,
                    "label": "caterpillars",
                    "pressure": 3,
                    "startTime": 1556046680.620079,
                    "endTime": 1556046694.53172,
                    "side": "left"
                  }
                },
                "eventSource": "aws:kinesis",
                "eventVersion": "1.0",
                "eventID": "shardId-000000000004:49594210671678830695451044071707598249733934195629097026",
                "eventName": "aws:kinesis:record",
                "invokeIdentityArn": "arn:aws:iam::425695594515:role/lambda-kinesis-role",
                "awsRegion": "us-west-2",
                "eventSourceARN": "arn:aws:kinesis:us-west-2:425695594515:stream/kinesis-RawDataStream-1R0K6AS5TFUIH"
              }
            }
        """


class CloudProviderFactory():
    """
    A class to create instacnes of CloudProviderBase.
    This is implemented as a singleton, so only one instance is allowed.
    Usage:
        RawProcessorProxyObj =  RawProcessorProxy.get_instance()

        Initializing:
        RawProcessorProxyObj.register(provider_name, provider_class)

        Creating instances:
        RawProcessorProxyObj.create(provider_name)
    """
    __instance = None
    _providers = {}

    def __init__(self):
        raise Exception("ERROR: Trying to get new instance for CloudProviderFactory. Try to use class methods like create or register")

    # Registers a cloud provider
    @classmethod
    def register(cls, provider_name, provider_class):
        if not issubclass(provider_class, CloudProviderBase):
            raise Exception(f"Wrong cloud provider class [{provider_class}]. The cloud provider class must be a descendent of CloudProviderBase.")
        cls._providers[provider_name] = provider_class()

    # Creates a provider instance
    @classmethod
    def get_provider(cls, provider_name):
        if provider_name in cls._providers:
            return cls._providers[provider_name]
        else:
            raise Exception("Invalid cloud provider name.")
