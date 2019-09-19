# b2-record-processor
The Record Prcessor processes incoming events from b2-raw-processor.

## Messages

### Input
[Refer to b2-raw-processor output message](https://github.com/ecoation/b2-s3-writer)


### Output

<details><summary>Example</summary>
<p>

```json
{
  "version": "1.0",
  "source": "s3://eis-b2-upload-dev/wave/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/0548207774501389-1558559456908-20190522-0-000000.tar.bz2",
  "valid": true,
  "incoming_timestamp": "2019-05-24 06:46:27",
  "outgoing_timestamp": "2019-05-28 18:55:00",
  "type": "wave",
  "filename": "wave/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/0548207774501389-1558559456908-20190522-0-000000.tar.bz2",
  "payload": [
    "record-processor/process_date=2019-05-28/datatype=wave/process_id=18084810-817a-11e9-9a88-da39be9c2ea0/record-processor__18084810-817a-11e9-9a88-da39be9c2ea0__18086b1a-817a-11e9-9a88-da39be9c2ea0.csv.gz"
  ],
  "customer_id": "c14913a7-3fc1-4764-a856-86fdaa82dcd9",
  "farm_id": "19fa24cc-e72b-4f9c-9e7f-d39a93090d7d",
  "phase_id": "5eb2eb5f-c509-4a5e-b353-66123bbbbcc3",
  "row_session_id": "0548207774501389_1558559515",
  "machine_id": "89c5d90e129902e4ba83af9d8c74a5e1",
  "hardware_version": "0.1",
  "firmware_version": "0.1",
  "row_location": {
    "tag_id": "0548207774501389",
    "row_number": 136,
    "post_number": 0,
    "side": "left",
    "direction": "0"
  }
}
```

</p>
</details>

<details><summary>JSON Schema</summary>
<p>


```json
 {
   "$schema": "http://json-schema.org/draft-07/schema#",
   "type": "object",
   "title": "S3 Writer Output",   
   "required": [
     "version",
     "source",
     "valid",
     "incoming_timestamp",
     "outgoing_timestamp",
     "type",
     "filename",
     "payload",
     "customer_id",
     "farm_id",
     "phase_id",
     "row_session_id",
     "machine_id",
     "hardware_version",
     "firmware_version",
     "row_location"
   ],
   "properties": {
     "version": {
       "type": "string",
       "examples": [
         "1.0"
       ],
     },
     "source": {
       "type": "string",
       "examples": [
         "s3://eis-b2-upload-dev/wave/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/0548207774501389-1558559456908-20190522-0-000000.tar.bz2"
       ],
     },
     "valid": {
       "type": "boolean",
       "examples": [
         true
       ]
     },
     "incoming_timestamp": {
       "type": "string",
       "examples": [
         "2019-05-24 06:46:27"
       ],
     },
     "outgoing_timestamp": {
       "type": "string",
       "examples": [
         "2019-05-28 18:55:00"
       ],
     },
     "type": {
       "type": "string",
       "description": "Type of file. The values are: wave, aux, image, video, label",
       "examples": [
         "wave"
       ],
     },
     "filename": {
       "type": "string",
       "examples": [
         "wave/89c5d90e129902e4ba83af9d8c74a5e1/2019-05-22/0548207774501389-1558559456908-20190522-0-000000.tar.bz2"
       ],
     },
     "payload": {
       "type": "array",
       "description": "A list of stage files. The value of each item is an S3 Key",
       "items": {
         "type": "string",
         "examples": [
           "record-processor/process_date=2019-05-28/datatype=wave/process_id=18084810-817a-11e9-9a88-da39be9c2ea0/record-processor__18084810-817a-11e9-9a88-da39be9c2ea0__18086b1a-817a-11e9-9a88-da39be9c2ea0.csv.gz"
         ],
       }
     },
     "customer_id": {
       "type": "string",
       "default" : "undefined",
       "examples": [
         "c14913a7-3fc1-4764-a856-86fdaa82dcd9"
       ],
     },
     "farm_id": {
       "type": "string",
       "default" : "undefined",
       "examples": [
         "19fa24cc-e72b-4f9c-9e7f-d39a93090d7d"
       ],
     },
     "phase_id": {
       "type": "string",
       "default" : "undefined",
       "examples": [
         "5eb2eb5f-c509-4a5e-b353-66123bbbbcc3"
       ],
     },
     "row_session_id": {
       "type": "string",
       "default" : "undefined",
       "examples": [
         "0548207774501389_1558559515"
       ],
     },
     "machine_id": {
       "type": "string",
       "default" : "undefined",
       "examples": [
         "89c5d90e129902e4ba83af9d8c74a5e1"
       ],
     },
     "hardware_version": {
       "type": "string",
       "default" : "undefined",
       "examples": [
         "0.1"
       ],
     },
     "firmware_version": {
       "type": "string",
       "default" : "undefined",
       "examples": [
         "0.1"
       ],
     },
     "row_location": {
       "type": "object",
       "required": [
         "tag_id",
         "row_number",
         "post_number",
         "side",
         "direction"
       ],
       "properties": {
         "tag_id": {
           "type": "string",
           "default" : "undefined",
           "examples": [
             "0548207774501389"
           ],
         },
         "row_number": {
           "type": "integer",
           "default" : 0,
           "examples": [
             136
           ]
         },
         "post_number": {
           "type": "integer",
           "default" : 0,
           "examples": [
             0
           ]
         },
         "side": {
           "type": "string",
           "default" : "undefined",
           "examples": [
             "left"
           ],
         },
         "direction": {
           "type": "string",
           "default" : "undefined",
           "examples": [
             "0"
           ],
         }
       }
     }
   }
 }
```

</p>
</details>
