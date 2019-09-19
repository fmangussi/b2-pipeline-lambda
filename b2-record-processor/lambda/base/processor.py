# -*- coding: utf-8 -*-
# Ecoation RawProcessorBase
#
# Created by: Farzad Khandan (farzadkhandan@ecoation.com)
#
from abc import ABC, abstractmethod

from helpers import get_logger


class ProcessorBase(ABC):

    # Initialization
    # Parameters:
    #   name: A generic name for the processor
    #   cloud_provider: an instance of RawCloudProvider class to interact with the underlying cloud service
    def __init__(self, name, cloud_provider):
        self._name = name
        self._cloud_provider = cloud_provider
        self._logger = get_logger(self.__class__.__name__)

    @property
    def logger(self):
        return self._logger

    # Process a Raw event
    # Parameters:
    #   event: a typical Lambda Event
    # Returns:
    #   {
    #       'processed': is the event processed?,
    #       'continue': should the event hand over to other processors?,
    #       'errors': a list of erros strings,
    #       'messages': a list of messages
    #   }
    @abstractmethod
    def process(self, event):
        return {
            'processed': False,
            'continue': True,
            'errors': [],
            'messages': []
        }