#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 18:28:24 2019

@author: fernando@ecoation.com
"""

json_schema = { 'type': 'object',
             'required': ['customer_id','farm_id','phase_id','capture_timestamp',
                                              'capture_local_datetime', 'upload_timestamp',
                                              'row_session_id', 'record_type',
                                             'row_location', 'cartesian_location', 'crops',
                                             'machine_id', 'hardware_version',
                                             'firmware_version',
                                             'label_meta'
                                             ],
            'properties': {         
                
        'customer_id': {
        "type": "string",
        "additionalProperties": False
    },
      'farm_id':  {
        "type": "string",
        
        "additionalProperties": False
    },
      'phase_id': {
        
        "type": "string",
        "additionalProperties": False
    },
     'capture_timestamp': { 
        "type": "number",    
        "additionalProperties": False
    },
     'capture_local_datetime': { 
        "type": "string",
        "additionalProperties": False
    },
     
    'upload_timestamp': {
        "type": "number",
        "additionalProperties": False
    },
    
    'row_session_id': { 
        "type": "string",
        "additionalProperties": False
    
    },
    'record_type': {
            "type": "string",
            "additionalProperties": False
            },
    'row_location': {
            "type": "object",
            "required": ["tag_id", "row_number", "distance_cm", 
                         "height_cm", "post_number", "velocity",
                         "side", "direction"],
        "properties": {
            "tag_id": {
                "type": "string",
                "additionalProperties": False
            },
            "distance_cm": {
                "type": "number",
                "additionalProperties": False
            },
            "height_cm": {
                "type": "number",
                "additionalProperties": False
            },
            "velocity": {
                "type": "number",
                "additionalProperties": False
            },
            "post_number": {"type": "integer",
                     "additionalProperties": False
            },
            "side": { "type": "string",
                         "additionalProperties": False
            },
            "direction": {"type": "string",
                      "additionalProperties": False 
            },
            "row_number": { "type": "integer",
                          "additionalProperties": False }
            
        },
        "additionalProperties": False
                },
            
    'cartesian_location': {
            "type": "object",
            "required": ["x","y","z"],
            "properties": {
            "x": {
                "type": "integer",
                "additionalProperties": False
            },
            "y": {
                "type": "integer",
                "additionalProperties": False
            },
            "z": {
                "type": "integer",
                "additionalProperties": False
            }
                    
            },
            
        "additionalProperties": False
            
     
                    },
     'crops': {
             "type":"array",
             "additionalProperties": False },
     'machine_id': {
                    "type":"string",
                    "additionalProperties": False},
     'hardware_version': {
             
             "type":"string",
             "additionalProperties": False},
     'firmware_version': {
             "type":"string",
             "additionalProperties": False},
     
    
     'label_meta': {
             "type":"object",
             "additionalProperties": True},
        
    },
    "additionalProperties": False  
      
    }