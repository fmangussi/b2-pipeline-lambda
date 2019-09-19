# Definition

Please refer to the backend configuration entity model and implementation documents:

* [Configuration Entities - Logical Design](https://docs.google.com/document/d/1v3hAZVxyyfT2v5bvpBT0LsZDmQ1VLq6jZSd62FvA5_g)
* [Configuration Entities - Physical Design](https://docs.google.com/document/d/1xL9z_lHRXBUut3ci0HS6C9XtJxlLBzBezQm4qwP0D_g)



# Models
  - [CacheGeneralModel](#cachegeneralmodel)
    - [Properties](#properties)
    - [Methods](#methods)
  - [CacheImageModel](#cacheimagemodel)
    - [Properties](#properties-1)
    - [Methods](#methods-1)
  - [ConfigSetModel](#configsetmodel)
    - [Properties](#properties-2)
    - [Methods](#methods-2)
  - [CustomerModel](#customermodel)
    - [Properties](#properties-3)
    - [Methods](#methods-3)
  - [FarmModel](#farmmodel)
    - [Properties](#properties-4)
    - [Methods](#methods-4)
  - [FileModel](#filemodel)
    - [Properties](#properties-5)
    - [Methods](#methods-5)
  - [MachineModel](#machinemodel)
    - [Properties](#properties-6)
    - [Methods](#methods-6)
  - [PhaseModel](#phasemodel)
    - [Properties](#properties-7)
    - [Methods](#methods-7)
  - [ProcessedFileModel](#processedfilemodel)
    - [Properties](#properties-8)
    - [Methods](#methods-8)
  - [RecipeModel](#recipemodel)
    - [Properties](#properties-9)
    - [Methods](#methods-9)
  - [SysConfigModel](#sysconfigmodel)
    - [Properties](#properties-10)
    - [Methods](#methods-10)
  - [TriggerStateModel](#triggerstatemodel)
    - [Properties](#properties-11)
    - [Methods](#methods-11)
  
# Helpers
  - [Product Base](#product-base)
  - [SysCache Base](#syscache-base)
  - [SysConfig Base](#sysconfig-base)
  - [Utility](#utility)


# Reference

## CacheGeneralModel
A caching layer for fast access materialized views

### Properties
* cache_type: one of 
  * SUB_CMV_COVERAGE = 'CVG'
  * SUB_CMV_LABEL = 'LBL'
  * SUB_CMV_AUX = 'AUX'
  * SUB_CMV_FRUITCOUNT = 'FRC'
* phase_id
* date
* cache: The cache content, JSON
* creattion_datetime

### Methods
* \_\_init\_\_(s3_bucket=""): Class constructor. If you want to support very large objects in the cache, you need to pass s3_bucket in the parameters. This bucket is used for caching large objects.
* get_coverage(phase_id, date): Gets the coverage cache for a specific phase_id and date.
* get_coverage_dates(phase_id, from_date, to_date): Gets the dates in the date range where there is a coverage for a specific phase_id.
* get_label(phase_id, date): Gets the label cache for a specific phase_id and date.
* get_aux(phase_id, date): Gets the aux cache for a specific phase_id and date.
* get_aux_dates(phase_id, from_date, to_date): Gets the dates in the date range where there is aux data for a specific phase_id.
* get_fruit_count(phase_id, date): Gets the fruit count cache for a specific phase_id and date.
* new_coverage(phase_id = "",
        date = "",
        cache = {},
    ): Creates a coverage cache.
* new_label(phase_id = "",
        date = "",
        cache = {},
    ): Creates a label cache.
* new_aux(phase_id = "",
        date = "",
        cache = {},
    ): Creates an aux cache.
* new_fruit_count(phase_id = "",
        date = "",
        cache = {},
    ): Creates a fruit count cache.
* save(): Writes the Config Set to the underlying database.
* delete(): Deletes the Config Set from the underlying database.
* delete_phase(cache_type, phase_id): Deletes the entire cache for a specified phase.


## CacheImageModel
A caching layer for fast access to image paths

### Properties
* cache_type: SUB_CMV_IMAGE = 'IMG'
* phase_id
* date
* camera
* x
* y
* image_path
* creattion_datetime

### Methods
* \_\_init\_\_(): Class constructor.
* get(phase_id, date, camera, x, y): Gets the image path.
* new(phase_id = "",
        date = "",
        camera = "",
        x = 0,
        y = 0,
        image_path = "",
        creation_date = ""
    ): Creates an image path cache.
* save(): Writes the Config Set to the underlying database.
* save_batch(batch): Writes a batch of image paths to the underlying database. 
  batch: a list of CacheImageModel() objects.
* delete(): Deletes the Config Set from the underlying database.
* delete_phase(phase_id): Deletes the entire cache for a specified phase.


## ConfigSetModel
A Machine Configuration Set

### Properties
* config_id
* config_name
* creation_date
* last_config_update
* sensor_side
  - Support for sensorSide, binary: bit 0 : left , bit 1 : right. Values are: 0-None 1-Left 2-Right 3-Both
* aux_sensors
  - Aux Sensors. the format is: [as1, as2, ...]
* recipe
  - The recipe assigned to this config set. The format is: 
```
        {
            'recipe_id': <value>, 
            'recipe_name': <value>
        }
```
* labels 
    - A set of Label() classes.
```
    class Label():
        def __init__(self):
    * label
    * category
```

### Methods
* \_\_init\_\_(): Class constructor.
* get_list(name_only = True): Get a list of Config Sets with some attributes. If name_only is true, return only the names.
* get_list_json(): Returns a list of Config Sets with some attributes as a JSON object.
* get(config_id): Retrieves a Config Set by its config_id.
* new(
        config_name = "",
        creation_date = "",
        last_config_update = "",
        sensor_side = 0,
        aux_sensors = [],
        recipe = {
            'recipe_id': "",
            'recipe_name': ""
        },
        labels = []     
    ): Creates a new empty Config Set.
* save(): Writes the Config Set to the underlying database.
* delete(): Deletes the Config Set from the underlying database.

## CustomerModel
A Customer

### Properties
* customer_id
* customer_name
* crm_id
* alias
* phone
* address
* logo
* creation_date
* default_timezone
* account_type
* owner_email
* is_active
* data_status

### Methods
* \_\_init\_\_(): Class constructor.
* get_list(return_null_customer=False): Returns list of customers. \<null customer\> is a special case handled by Backend to maintain unassigned machines. If return_null_customer is True, it also returns the \<null customer\> in the list.
* get_list_json(, return_null_customer=False): Returns list of customers as a JSON file. \<null customer\> is a special case handled by Backend to maintain unassigned machines. If return_null_customer is True, it also returns the \<null customer\> in the list.
* get(customer_id): Retrieves a Customer by its customer_id.
* get_by_crm_id(crm_id): Retrieves a Customer by its crm_id.
* new(
        customer_name = "",
        alias = "",
        phone = "",
        address = "",
        logo = "",
        default_timezone = "",
        account_type = "Standard",
        owner_email = "",
        is_active = False
    ): Creates a new empty Customer.
* save(): Writes the entity to the underlying database.
* delete(): Deletes the entity from the underlying database.
* data_incoming(): Marks the entity to have INCOMING data.
* data_processed(): Marks the entity to have PROCESSED data.
* data_archived(): Marks the entity to have ARCHIVED data.

## FarmModel
A Customer's Farm

### Properties
* customer_id
* farm_id
* farm_name
* phone
* address
* timezone
* data_status

### Methods
* \_\_init\_\_(): Class constructor.
* get_list(customer_id): Gets a list of farms owned by a customer with customer_id.
* get_list_json(customer_id): Gets a list of farms owned by a customer with customer_id as a JSON string.
* get_list_all(): Returns a list of all the farms in the system.
* get(customer_id, farm_id):  Retrieves a Farm owned by a Customer.
* get_by_farm_id(farm_id): Retrieves a Farm by its farm_id.
* new(
        customer_id,
        farm_name = "",
        phone = "",
        address = "",
        timezone = "",
    ): Created an empty Farm object.
* save(): Writes the entity to the underlying database.
* delete(): Deletes the entity from the underlying database.
* data_incoming(): Marks the entity to have INCOMING data.
* data_processed(): Marks the entity to have PROCESSED data.
* data_archived(): Marks the entity to have ARCHIVED data.

## FileModel
An uploaded and processed file. It is used to keep track of Excel Files which are used to create Phases.

### Properties
* customer_id
* farm_id
* file_id
* file_name
* file_ext
* phases
  - A list of phase_ids of the phases which were created/configured using this file.
* creation_date
* content
  - Actual file content

### Methods
* \_\_init\_\_(): Class constructor.
* get_list(customer_id, farm_id): Gets a list of files which was used to create phases in a farm owned by a customer.
* get_list_json(customer_id, farm_id): Gets a list of files which was used to create phases in a farm owned by a customer as a JSON string.
* get(customer_id, farm_id, file_id):  Retrieves a File from a Farm owned by a Customer.
* get_by_farm_id(farm_id): Retrieves a Farm by its farm_id.
* new(customer_id = "",
        farm_id = "",
        file_id = "",
        file_name = "",
        file_ext = "",
        phases = [],
        content = None,
    ): Created an empty File object.
* save(): Writes the entity to the underlying database.
* delete(): Deletes the entity from the underlying database.

## MachineModel
An Ecoation Machine (Product)

### Properties
* customer_id
* machine_id
* serial_no
* machine_type
* pin
* is_active
* data_status
* config
  - The machine config set assigned to this machine, the format is:
```
    {
        config_id: <value>, 
        config_name: <value>
    }

```
* farms
  - The farms that this machine is assigned to, the format is:
```
    [
        {
            'farm_id': <value>, 
            'farm_name': <value>, 
            'timezone': <value>
        }
    ]
```

### Methods
* \_\_init\_\_(): Class constructor.
* get_list(customer_id): Gets a list of machines owned by a customer with customer_id.
* get_list_json(customer_id): Gets a list of machines owned by a customer with customer_id as a JSON string.
* get_list_all(): Returns a list of all the Machines in the system.
* get(customer_id, machine_id):  Retrieves a Machine owned by a Customer.
* get_by_id(machine_id): Retrieves a Machine by its machine_id.
* new(
        customer_id,
        serial_no = "",
        machine_type = "",
        pin = 0,
        is_active = "",
        config = {
            'config_id': "",
            'config_name': ""
        },
        farms = []     
    ): Creates an empty instance of the Machine.
* assign_to_customer(new_customer_id): Assigns te machine to a new customer. It automatically saves the data, no need to call save().
* save(): Writes the entity to the underlying database.
* delete(): Deletes the entity from the underlying database.
* data_incoming(): Marks the entity to have INCOMING data.
* data_processed(): Marks the entity to have PROCESSED data.
* data_archived(): Marks the entity to have ARCHIVED data.

## PhaseModel
A Phase in a Customer's Farm.

### Properties
* customer_id
* farm_id
* phase_id
* phase_name
* phase_type
* walkway_width
* rows
* posts
* disabled_posts
* bays
* is_active
* creation_date
* phase_status
* data_status

**rows** is a collection of *row* objects. *row* objects are dictionaries with these values:
  - row_key: The row key, it is side+row_number
  - row_number
  - tag_id
  - side
  - bay
  - row
  - row_offset
  - margin_right
  - max_height
  - min_height
  - row_length
  - row_width
  - margin_left
  - rail_distance
  - is_active
  - creation_date
  - crops: list of the crops in this row. The format is:
```
    [
        {
            'side': <value>,
            'crop': <value>,
            'variation': <value>,
        }
    ]    
```

### Methods
* \_\_init\_\_(): Class constructor.
* clear_posts(): Clear all posts.
* set_post(side, post_count, post_length): Sets/Adds a post in/to the phase.
* clear_disabled_posts(): Clear list of disabled posts.
* set_disabled_post(x, y): Sets/Adds a disabled post in/to the phase.
* clear_rows(): Clear the *rows* list.
* set_row(
        # row_key, # key <side>-<row_number> - must be unique in each phase
        row_number, 
        tag_id,
        side,
        bay,
        row,
        crops,          # (L) - side: S, crop: S, variation: S
        row_offset,
        margin_right,
        margin_left,
        max_height,
        min_height,
        row_length,
        row_width,
        rail_distance,
        is_active,
        creation_date=None,
    ): Sets/Adds a row in/to the phase.
* get_list_all(): Returns a list of all the Phases in the system.
* get_list_by_farm_id(farm_id, *arg): Get the list of the phases in a farm or a list of forms.
* get_list_by_farm_id_json(farm_id, *arg): Get the list of the phases in a farm or a list of forms as a JSON string.
* get_list_by_customer_id(customer_id): List all the phases owned by a customer.
* get(customer_id, farm_id, phase_id): Retrieves a Phase in a Farm owned by a Customer.
* find_row(tag_id, load_phase=False): Retrieves properties of a Row in the system, identified by its tag_id. If load_phase is true (default is False), then it also loads the phase object from the database. This speeds up the consequent calls to find_row if the program is processing a set of rows from the same phase. If this is not the case, leave load_phase to be False.
* get_by_id(phase_id): Retrieve the phase by its phase_id.
* new(customer_id,
        farm_id,
        phase_id="",
        phase_name="",
        phase_type="",
        walkway_width=0,
    ): Creates a new and empty Phase.
* compare_and_update(): Compares the entity in memory with the object in database, modifies or returns error based on the Phase Creation status in *phase_status*.
* set_status_active(): Set the Phase creation status to Active.
* save(): Writes the entity to the underlying database.
* delete(): Deletes the entity from the underlying database.
* data_incoming(): Marks the entity to have INCOMING data.
* data_processed(): Marks the entity to have PROCESSED data.
* data_archived(): Marks the entity to have ARCHIVED data.
* get_cartesian_location(row_number, side, distance_cm, height_cm): Returns the cartazian coordinate of a given sample location. Returns a dictionary:
```
  {
    "x": int(x),
    "y": int(y),
    "z": int(z)
  }
```


## ProcessedFileModel
Keeps track of a File that has been processed in the data pipeline.

### Properties
* bucket: The name of the bucket or “<undefined>” if not applicable.
* file_name
* last_stage: One of thses
  * STAGE_RAW_PROCESSED = "raw_processed"
  * STAGE_RECORD_PROCESSED = "record_processed"
  * STAGE_ATHENA_WRITTEN = "athena_written"
  * STAGE_MODEL_PROCESSED = "model_processed"
  * STAGE_KEYVALUE_STORED = "key_value_processed"
  * STAGE_INVALID_DUMPED = "invalid_dumped"
* last_stage_datetime
* log: The full operation log for this file.
* creation_date


### Methods
* \_\_init\_\_(): Class constructor.
* get(bucket, file_name): Gets the file.
* new(bucket = "",
        file_name = "",
        last_stage = "",
        last_stage_datetime = "",
        log = [],
        creation_date = ""
    ): Creates the file object.
* save(): Writes the Processed File to the underlying database.
* delete(): Deletes the Processed File from the underlying database.
* log_stage(self, last_stage, process_name, thetime=datetime.now(pytz.utc)): Logs an operation for the file.



## RecipeModel
A Recipe

### Properties
* recipe_id
* recipe_name
* creation_date
* last_recipe_update
* sampling_interval_secs
* pulse_enable
* pulse_flash_order
* pulse_flash_duty_down
* pulse_flash_duty_up
* pulse_intensity 
* pulse_integration_time
* pulse_number_of_scans
* solid_intensity 
* solid_enable
* solid_flash_order
* solid_integration_time

### Methods
* \_\_init\_\_(): Class constructor.
* get_list(return_null_customer=False): Returns list of Recipes. 
* get_list_json(return_null_customer=False): Returns list of Recipes as a JSON string.
* get(recipe_id): Retrieves a Recipe by its recipe_id.
* new(recipe_name = "",
        creation_date = "",
        last_recipe_update = "",
        sampling_interval_secs = 0,
        pulse_enable = False,
        pulse_flash_order = [],
        pulse_flash_duty_down = 0,
        pulse_flash_duty_up = 0,
        pulse_intensity = {},
        pulse_integration_time = 0,
        pulse_number_of_scans = 0,
        solid_intensity = {},
        solid_enable = False,
        solid_flash_order = [],
        solid_integration_time = 0
    ): Creates an empty new Recipe object.
* save(): Writes the entity to the underlying database.
* delete(): Deletes the entity from the underlying database.
* data_incoming(): Marks the entity to have INCOMING data.
* data_processed(): Marks the entity to have PROCESSED data.
* data_archived(): Marks the entity to have ARCHIVED data.

## SysConfigModel
General Sytem Configuration

### Properties
* config_date 
* crops
* label_categories
* machine_types
* account_types
* pulse_lights
* solid_lights
* aux_sensors
* crop_varieties: Crop Varieties, the format is:
```
    [
        CropVariety Class
    ]
```
* labels
    - A set of Label() classes.
```
    class Label():
        def __init__(self):
    * label
    * category
```

### Methods
* \_\_init\_\_(): Class constructor.
* read_config(): Reads the config from database.
* read_crop_varieties(): Reads the crop varieties settings from the database.
* read_labels(): Reads the Labels
* get_all(): Read the entire system config.
* save(): Writes the SysConfig to the database.

## TriggerStateModel
A cache keeping track of the states for triggering Live Models.

### Properties
* state_key
* state
* creation_date
* timeout

### Methods
* \_\_init\_\_(): Class constructor.
* get(state_key):  Retrieves a trigger state from the cache.
* new(state_key = "",
        state = "",
        creation_date = "",
        timeout = 0,
    ): Create an empty TriggerState object.
* save(): Writes the entity to the underlying database.
* delete(): Deletes the entity from the underlying database.

## Product Base

Utility class to support reading/writing backend customer/farm/machine configuration data from/to database (DynamoDB).

## SysCache Base

Utility class to support reading/writing backend data pipeline caches from/to database (DynamoDB)

## SysConfig Base

Utility class to support reading/writing backend system configuration data from/to database (DynamoDB)


