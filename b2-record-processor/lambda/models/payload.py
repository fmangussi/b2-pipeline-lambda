#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 15:21:50 2019

@author: mandeep
"""

from datetime import datetime
import dateutil.parser
import logging
logger = logging.getLogger()
logging.basicConfig(level='INFO')


class AbstractPayload:

    def __init__(self, payload_info, validity, incoming_json=None):
        self._logger = logger

        self.incoming_json = incoming_json
        self.invalidity_reason = validity['reason']

        self.event_record = payload_info['event_record']
        self.version = 1.0
        self.source = self.event_record['s3']['object']['key']
        self.valid = payload_info['validity']
        #incomingISO_eventtime =str(dateutil.parser.parse(self.event_record['eventTime']))
        self.incoming_eventtime = str(dateutil.parser.parse(self.event_record['eventTime'])).split('+')[0]

        self.filename = payload_info['filename']
        self.type = payload_info['type']
        self.full_filename = payload_info['full_filename']
        self.valid_json = None
        self.invalid_json = None
#        self.main()

    def create_valid_message_payload(self):

        self.valid_json = {
                            "version": self.version,
                            "source": self.source,
                            "valid": self.valid,
                            "incoming_timestamp": self.incoming_eventtime,
                            "outgoing_timestamp": str(datetime.utcnow().replace(microsecond=0)),

                            "type": self.type,
                            "filename": self.filename
                            }



    def create_invalid_message_payload(self):

        self.invalid_json = {
                            "version": self.version,
                            "source": self.source,
                            "valid": self.valid,
                            "incoming_timestamp": self.incoming_eventtime,
                            "outgoing_timestamp": str(datetime.utcnow().replace(microsecond=0)),

                            "type": self.type,
                            "filename": self.filename,
                            "reason": self.invalidity_reason,
                            "invalid_source": 'raw_processor'
                    }
