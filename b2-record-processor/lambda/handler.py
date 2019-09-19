#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# @author: Farzad Khandan (fazrdkhandan@ecoation.com)
#
import config
from helpers import get_logger
import register
from models import b_log
b_log.init(b_log.PROCESS_RECORD_PROCESSOR)

register.register_processors()
logger = get_logger(__name__)


def lambda_handler(event, context):
    message = "FAILED"
    records_processed = 0
    exception_count = 0

    try:
        print(f"event: {event}")
        # Main handler
        if 'Records' not in event:
            logger.error("Invalid event: %s", str(event))
            return

        for r in event['Records']:
            records_processed += 1
            try:
                if not r.get('kinesis',{}).get('data', ''):
                    raise KeyError("The kinesis/data was not found.")
                process_result = config.SYS_PROXY.process(r)
            except Exception as ex:
                process_result = 1
                exception_count += 1
                logger.error("An error occured while processing the event: %s", str(ex))
            exception_count += process_result
        message = "OK"
    finally:
        return {
            'message': message,
            'records_processed': records_processed,
            'exception_count': exception_count
        }

