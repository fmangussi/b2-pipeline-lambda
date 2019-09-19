#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 5 13:58:24 2019

@author: fernando@ecoation.com
"""

import fastjsonschema as fjs

schema_model_fruit_count_summary = fjs.compile({
    'type': 'object',
    'required': [
        'customer_id',
        'farm_id',
        'phase_id',
        'capture_timestamp',
        'capture_local_datetime',
        'upload_timestamp',
        'row_session_id',
        'record_type',
        'row_location',
        'cartesian_location',
        'crops',
        'machine_id',
        'hardware_version',
        'firmware_version',
        'model_metadata',
        ],
    'properties': {
        'customer_id': {'type': 'string',
                        'additionalProperties': True},
        'farm_id': {'type': 'string', 'additionalProperties': True},
        'phase_id': {'type': 'string', 'additionalProperties': True},
        'capture_timestamp': {'type': 'number',
                              'additionalProperties': True},
        'capture_local_datetime': {'type': 'string',
                                   'additionalProperties': True},
        'upload_timestamp': {'type': 'number',
                             'additionalProperties': True},
        'row_session_id': {'type': 'string',
                           'additionalProperties': True},
        'record_type': {'type': 'string',
                        'additionalProperties': True},
        'row_location': {
            'type': 'object',
            'required': [
                'tag_id',
                'row_number',
                'distance_cm',
                'height_cm',
                'post_number',
                'velocity',
                'side',
                'direction',
                ],
            'properties': {
                'tag_id': {'type': 'string',
                           'additionalProperties': True},
                'distance_cm': {'type': 'number',
                                'additionalProperties': True},
                'height_cm': {'type': 'number',
                              'additionalProperties': True},
                'velocity': {'type': 'number',
                             'additionalProperties': True},
                'post_number': {'type': 'integer',
                                'additionalProperties': True},
                'side': {'type': 'string',
                             'additionalProperties': True},
                'direction': {'type': 'string',
                              'additionalProperties': True},
                'row_number': {'type': 'integer',
                               'additionalProperties': True},
                },
            'additionalProperties': True,
            },
        'cartesian_location': {
            'type': 'object',
            'required': ['x', 'y', 'z'],
            'properties': {'x': {'type': 'integer',
                           'additionalProperties': True},
                           'y': {'type': 'integer',
                           'additionalProperties': True},
                           'z': {'type': 'integer',
                           'additionalProperties': True}},
            'additionalProperties': True,
            },
        'crops': {'type': 'array', 'additionalProperties': True},
        'machine_id': {'type': 'string', 'additionalProperties': True},
        'hardware_version': {'type': 'string',
                             'additionalProperties': True},
        'firmware_version': {'type': 'string',
                             'additionalProperties': True},
        'model_metadata': {'type': 'object',
                           'additionalProperties': True,
                           'required': [
                               'color',
                               'direction',
                               'count',
                               'result_utc_local_datetime'
                           ],
                           'properties': {
                               'color': {'type': 'string'},
                               'direction': {'type': 'string', "minLength": 1},
                               'count': {'type': 'integer'},
                               'result_utc_local_datetime': {'type': 'string', "minLength": 1}
                           }},
        'capture_local_date': {'type': 'string',
                               'additionalProperties': True},
        'version': {'type': 'string', 'additionalProperties': True},
        },
    'additionalProperties': True,
    })
