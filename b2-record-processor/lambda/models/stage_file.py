import ast
import csv
import json

import pandas as pd

from helpers import get_logger

logger = get_logger(__name__)

CSV_DELIMITER = ';'
CSV_ESCAPECHAR = "\\"
CSV_QUOTECHAR = '"'
CSV_QUOTING = csv.QUOTE_NONNUMERIC
CSV_STRICT = True

INPUT_DTYPES = {
    'farm_id': str,
    'tag_id': str,
    'rsid': str,
    'phase_id': str,
    'capture_timestamp': str,
    'capture_local_datetime': str,
    'upload_timestamp': str,
    'row_session_id': str,
    'record_type': str,
    'row_location': str,
    'cartesian_location': str,
    'crops': str,
    'machine_id': str,
    'hardware_version': str,
    'firmware_version': str,
    'version': str,
    'customer_id': str,
    'capture_local_date': str,
    'sensor_meta': str,
    'sensor_data': str,
    'sensor_recipe': str,
    'wave_file_path': str,
    'video_file_path': str,
    'video_frame': str,
    'camera': str,
    'camera_metadata': str,
    'image_file_path': str,
    'label_meta': str,
    'tagId': str,
    'startTime': str,
    'endTime': str,
    'startDistance': str,
    'endDistance': str,
    'side': str,
    'label': str,
    'pressure': str,
    'seq': str,
    'id': str,
    'location': str,
    'frames': str,
    'time': str,
    'instance': str,
    'param': str,
    'value': str,
    'distance': str,
    'tagid': str,
    'velocity': str,
    'height': str,
    'counter': str,
    'probe': str,
    'serial': str,
    'wavelength': str,
    'image_name': str,
    'seg': str,
    'node': str,
    'co2': str,
    'namespace': str,
    'variance': str,
    'relative_humidity': str,
    'temperature': str,
    'par': str,
    'yaw': str,
    'pitch': str,
    'roll': str,
    'power_supply_health': str,
    'cell_voltage': str,
    'present': str,
    'capacity': str,
    'power_supply_technology': str,
    'design_capacity': str,
    'power_supply_status': str,
    'current': str,
    'charge': str,
    'percentage': str,
    'voltage': str,
    'row_number': str,
    'color': str,
    'count': int,
    'color_code': str,
    'datetime': str,
    'direction': str,
    'greenhouse_side': str,
    'filename': str,
    'flower_id': str,
    'h': int,
    'predictions': str,
    'probability': str,
    'result_utc_local_datetime': str,
    'row_side': str,
    'tomato_id': str,
    'w': int,
    'x': int,
    'y': int
}


class StageCSVReaderFile:

    filepath: str
    _data_frame: pd.DataFrame
    _current_index: int
    _max_index: int

    def __init__(self, filepath):
        try:
            try:
                dtype = INPUT_DTYPES.copy()
                header = pd.read_csv(filepath, dtype=INPUT_DTYPES, nrows=1)
                for colname in header.columns:
                    if colname not in dtype.keys():
                        dtype[colname] = str
            except Exception as error:
                logger.info('Exception while trying to process the header file on StageCSVReaderFile.__init__ method.')
                dtype = INPUT_DTYPES
            self.filepath = filepath
            self._data_frame = pd.read_csv(filepath, dtype=dtype)
            self._data_frame = self._data_frame.astype('object')
            self._max_index = len(self.data_frame.index) - 1
            self._current_index = -1

        except FileNotFoundError:
            raise FileNotFoundError("The stage csv file path was not found: [%s]" % filepath)

    @property
    def size(self) -> int:
        return self._max_index + 1

    @property
    def data_frame(self):
        return self._data_frame

    def is_index_valid(self, index, raise_exception=False):
        if raise_exception and index > self._max_index:
            raise IndexError("The index [%d] is out-of-bounds [%d]" % (index, self._max_index))
        return index <= self._max_index

    def seek(self, index):
        self.is_index_valid(index, raise_exception=True)
        self._current_index = index

    def __iter__(self):
        return self

    def __len__(self):
        return self.size

    @staticmethod
    def eval_elements(row: pd.Series):
        try:
            for col_name in list(row.index):
                if pd.isnull(row[col_name]):
                    row[col_name] = None
                col_value = row[col_name]
                try:
                    if col_value and col_value.__class__ is str and col_value[0:1] in ['{', '[']:
                        eval_value = ast.literal_eval(col_value)  # convert string into dict
                        if eval_value.__class__ in (dict, list):
                            row = row.copy()
                            row[col_name] = eval_value
                except Exception as error:
                    logger.error(f"Stage line is invalid. Details: {{ 'row': '{row}' }}. Details: {error}")
            row = row.to_dict()
            if len(row.keys()) == 1 and "payload" in row:
                row = row["payload"]
        except Exception as error:
            logger.error("Error while eval_elements. Details: %s" % error)

        finally:
            return row

    def __next__(self):
        if not self.is_index_valid(index=self._current_index + 1, raise_exception=False):
            raise StopIteration
        self._current_index += 1
        return self.eval_elements(self.data_frame.iloc[self._current_index])


class StageCSVWriterFile:
    _data_frame : pd.DataFrame
    type_file : str
    unique_col_name = ["col_0"]
    LIST_TYPE_FILE = 'list'
    DICT_TYPE_FILE = 'dict'

    def __init__(self, filename):
        self.filename = filename
        self._data_frame = None
        self.type_file = ""
        self._list_content = []
        self.col_names = None
        self.is_closed = False

    @staticmethod
    def create_data_frame(columns: list, data=None):
        return pd.DataFrame(data=data, columns=columns)

    def add_row(self, row: dict):
        if self.type_file is not self.DICT_TYPE_FILE:
            raise ValueError("This instance only supports %s as input content. Details: %s" % (self.type_file, row.__class__.__name__))
        self.list_content.append(row)
        return 1

    def add_rows(self, rows: list):
        if self.type_file is not self.LIST_TYPE_FILE:
            raise ValueError("This instance only supports %s as input content. Details: %s" % (self.type_file, rows.__class__.__name__))
        self.list_content = self.list_content + rows
        return len(rows)

    def add_content(self, content):
        if content and content.__class__ is list:
            return self.add_rows(rows=content)
        if content and content.__class__ is dict:
            return self.add_row(row=content)

    @property
    def list_content(self):
        return self._list_content

    @list_content.setter
    def list_content(self, value):
        self._list_content = value

    @property
    def data_frame(self) -> pd.DataFrame:
        return self._data_frame

    @data_frame.setter
    def data_frame(self, value):
        self._data_frame = value

    @property
    def emptyfile(self):
        return len(self.list_content) == 0

    def write(self, content):
        if not content:
            raise ValueError("The input content is empty or null.")
        if content.__class__ not in [dict, list]:
            raise ValueError("The class [%s] is not supported. Supported: list, dict. Content: <%s>" % (content.__class__, content))
        self.raise_if_close()
        if self.emptyfile:
            self.type_file = self.LIST_TYPE_FILE
            self.col_names = self.unique_col_name
            if content.__class__ is dict:
                self.type_file = self.DICT_TYPE_FILE
                self.col_names = list(content.keys())
        self.add_content(content)

    def raise_if_close(self):
        if self.is_closed:
            raise ValueError("I/O operation on closed file.")

    def close(self):
        def convert_to_json(x):
            try:
                return json.dumps(x)
            except TypeError:
                return json.dumps(eval(str(x)))

        self.raise_if_close()
        if self.type_file is self.LIST_TYPE_FILE:
            self.data_frame = self.create_data_frame(data=None, columns=self.unique_col_name)
            self.data_frame[self.unique_col_name[0]] = self.list_content
        if self.type_file is self.DICT_TYPE_FILE:
            self.data_frame = self.create_data_frame(data=self.list_content, columns=self.col_names)
        result = len(self.list_content)
        self.list_content = []
        self.is_closed = True
        df = self.data_frame
        row = df.iloc[0]
        for col_name in list(df.columns):
            if row[col_name].__class__ in [dict, list]:
                try:
                    df[col_name] = df[col_name].apply(convert_to_json)
                except Exception as error:
                    error_message = f"Error while closing the stage file. Details: col_name is <{col_name}> row[{col_name}].class {row[col_name].__class__}"
                    logger.error(error_message)
                    raise


        return result
#
# if __name__ == "__main__":
#     import pprint
#     s = 's3://eis-b2-staging-data-dev/raw-processor/process_date=2019-06-09/datatype=aux/0548207774491915-1558550387142-20190522-auxsensorbox_co2-184951.tar.bz2__d949b9f6-8ad4-11e9-84ab-6aa4804b18f2.csv.gz'
#     #s = 's3://eis-b2-staging-data-dev/raw-processor/process_date=2019-06-09/datatype=wave/0548207774491915-1558550387142-20190522-0-000000.tar.bz2__0548207774491915-1558550387142-20190522-0-184043.wave__25cf5af0-8ad6-11e9-97e0-c2c8ea5d979c.csv.gz'
#     f = StageCSVReaderFile(s)
#     for r in f:
#         pprint.pprint(r)

    # swf = StageCSVWriterFile(
    #     processorname="test_processor",
    #     processid="1234-123456",
    #     filename="upload.tar.bz2")
    # swf.write({"sensor_name": "saber", "value": 1234})
    # swf.write({"sensor_name": "humidity", "value": 5678})
    # print(swf.close())