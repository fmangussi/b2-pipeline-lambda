# Database Models
# ---------------------------------------------
# Phase Model
#
import re
import uuid
from datetime import datetime

import pytz

from .config import *
from .product_base import (DATA_STATUS_ARCHIVED,
                           DATA_STATUS_FRESH, DATA_STATUS_INCOMING,
                           DATA_STATUS_PRCOESSED, DATA_STATUS_VALID_VALUES,
                           PARTITION_KEY, PHASE_STATUS_ACTIVE,
                           PHASE_STATUS_CREATED, PHASE_STATUS_NONE, SORT_KEY,
                           ProductHelper, KEY_DELIM)
from .sysconfig_base import SysConfigHelper
from .utility import get_attr, jsonstr, safe_int, send_update_message

STRUCTURE_PHASE_KEYS = [
    "post_length",
    "num_posts",
    "type",
]

STRUCTURE_ROW_KEYS = [
    "row_number", 
    "row",
    "bay",
]

class ERR_PHASE_NOT_FOUND(Exception):
    pass

class ERR_INVALID_RECORD(Exception):
    pass

class ERR_DELETE(Exception):
    pass

class PhaseModel():
    """
    A Phase in a Customer's Farm
    """
    def __init__(self):
        # Machine attributes
        self.customer_id = ""
        self.farm_id = ""
        self.phase_id = ""
        self.phase_name = ""
        self.phase_type = ""
        self.walkway_width = 0
        self.rows = []
        self.posts = []
        self.disabled_posts = []
        self.bays = {}
        self.is_active = False
        self.creation_date = ""
        self.phase_status = ""
        self.data_status = DATA_STATUS_FRESH
        self._rows_changed = False

        # Product Helper
        self._ph = ProductHelper(DYN_PRODUCT, AWS_REGION, MODULE_LOGGER)

        # SysConfig Helper
        self._sc = SysConfigHelper(DYN_CONFIG, AWS_REGION, MODULE_LOGGER)

    def clear_posts(self):
        self.posts = []

    def set_post(self, side, post_count, post_length):
        try:
            if not self.posts:
                self.posts = []
        except:
            self.posts = []

        zp = next(iter([x for x in self.posts if x['side'] == side]), None)
        if not zp:
            zp = {}
            self.posts.append(zp)

        zp['side'] = side
        zp['post_count'] = safe_int(post_count)
        zp['post_length'] = safe_int(post_length)

    def clear_disabled_posts(self):
        self.disabled_posts = []

    def set_disabled_post(self, x, y):
        try:
            if not self.disabled_posts:
                self.disabled_posts = []
        except:
            self.disabled_posts = []

        zp = { 'x': x, 'y': y }
        if zp not in self.disabled_posts:
            self.disabled_posts.append(zp)

    def clear_rows(self):
        self.rows = []
        self._rows_changed = True

    def set_row(self,
        # row_key, # key <side>-<row_number> - must be unique in each phase
        row_number, 
        tag_id,
        side,
        bay,
        row,
        crops, # (L) - side: S, crop: S, variation: S
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
    ):
        row_key = f"{side}-{row_number}"
        zr = next(iter([x for x in self.rows if x['row_key'] == row_key]), None)
        if not zr:
            zr = {}
            self.rows.append(zr)

        zr['row_key'] = row_key
        zr['row_number'] = safe_int(row_number)
        zr['tag_id'] = tag_id
        zr['side'] = side
        zr['bay'] = bay
        zr['row'] = row
        # Just to make sure the structure is ok
        zr['crops'] = []
        for c in crops:
            zr['crops'].append({
                'side': c['side'],
                'crop': c['crop'],
                'variation': c['variation'],
            })
        zr['row_offset'] = safe_int(row_offset)
        zr['margin_right'] = safe_int(margin_right)
        zr['max_height'] = safe_int(max_height)
        zr['min_height'] = safe_int(min_height)
        zr['row_length'] = safe_int(row_length)
        zr['row_width'] = safe_int(row_width)
        zr['margin_left'] = safe_int(margin_left)
        zr['rail_distance'] = safe_int(rail_distance)
        zr['is_active'] = is_active
        if not get_attr(zr, 'creation_date'):
            if creation_date: 
                zr['creation_date'] = creation_date
            else:
                zr['creation_date'] = str(datetime.now(pytz.utc))        
        self._rows_changed = True

    def get_list_all(self):
        obj = self._ph.get_all_phases_list()
        result = []
        for phase in obj:
            o = PhaseModel()
            __, customer_id = phase[PARTITION_KEY].split('#')
            __, farm_id, __, phase_id = phase[SORT_KEY].split('#')
            o.customer_id = customer_id
            o.farm_id = farm_id
            o.phase_id = phase_id
            o.phase_name = phase['phase_name']
            o.phase_type = phase['phase_type']
            o.creation_date = get_attr(phase, 'creation_date')
            o.phase_status = get_attr(phase, 'phase_status', PHASE_STATUS_NONE)
            o.data_status = get_attr(phase, 'data_status', DATA_STATUS_FRESH)
            result.append(o)
        return result

    def get_list_by_farm_id(self, farm_id, *arg):
        obj = self._ph.get_phases_list(farm_id, *arg)
        result = []
        for phase in obj:
            o = PhaseModel()
            __, customer_id = phase[PARTITION_KEY].split('#')
            __, farm_id, __, phase_id = phase[SORT_KEY].split('#')
            o.customer_id = customer_id
            o.farm_id = farm_id
            o.phase_id = phase_id
            o.phase_name = phase['phase_name']
            o.phase_type = phase['phase_type']
            o.creation_date = get_attr(phase, 'creation_date')
            o.phase_status = get_attr(phase, 'phase_status', PHASE_STATUS_NONE)
            o.data_status = get_attr(phase, 'data_status', DATA_STATUS_FRESH)
            result.append(o)
        return result

    def get_list_by_farm_id_json(self, farm_id, *arg):
        obj = self._ph.get_phases_list(farm_id, *arg)
        # print('PHASE LIST - ', obj)
        result = []
        for phase in obj:
            __, customer_id = phase[PARTITION_KEY].split('#')
            __, farm_id, __, phase_id = phase[SORT_KEY].split('#')
            o = {
                'customer_id': customer_id,
                'farm_id': farm_id,
                'phase_id': phase_id,
                'phase_name': phase['phase_name'],
                'phase_type': phase['phase_type'],
                'creation_date': get_attr(phase, 'creation_date'),
                'phase_status': get_attr(phase, 'phase_status', PHASE_STATUS_NONE),
                'data_status': get_attr(phase, 'data_status', DATA_STATUS_FRESH),
            }
            result.append(o)
        return jsonstr(result)

    def get_list_by_customer_id(self, customer_id):
        farms = self._ph.get_customer_farm_list(customer_id)
        if len(farms) == 0:
            return None

        farm_ids = []
        for x in farms:
            __, farm_id = x[SORT_KEY].split(KEY_DELIM)
            farm_ids.append(farm_id)
        # print("Farmd IDs: ", farm_ids)
        obj = self._ph.get_phases_list(*farm_ids)
        # print("Result: ", obj)
        result = []
        for phase in obj:
            o = PhaseModel()
            __, customer_id = phase[PARTITION_KEY].split('#')
            __, farm_id, __, phase_id = phase[SORT_KEY].split('#')
            o.customer_id = customer_id
            o.farm_id = farm_id
            o.phase_id = phase_id
            o.phase_name = phase['phase_name']
            o.phase_type = phase['phase_type']
            o.creation_date = get_attr(phase, 'creation_date')
            o.phase_status = get_attr(phase, 'phase_status', PHASE_STATUS_NONE)
            o.data_status = get_attr(phase, 'data_status', DATA_STATUS_FRESH)
            result.append(o)
        return result

    def get(self, customer_id, farm_id, phase_id):
        obj = self._ph.get_customer_phase_full(customer_id, farm_id, phase_id)
        if obj == None:
            raise(ERR_PHASE_NOT_FOUND(f"Phase not found: {customer_id} - {farm_id} - {phase_id}"))

        phases = [m for m in obj if re.match('F#[A-Za-z0-9-_]+#P#[A-Za-z0-9-_]+$', m[SORT_KEY])]
        if len(phases) == 0:
            return None

        phase = phases[0]

        self.customer_id = customer_id
        self.farm_id = farm_id
        self.phase_id = phase_id
        self.phase_name = phase['phase_name']
        self.phase_type = phase['phase_type']
        self.walkway_width = safe_int(get_attr(phase, 'walkway_width'))
        
        self.posts = phase['posts']
        # change Decimals to Int
        for x in self.posts:
            x['post_count'] = int(x['post_count'])
            x['post_length'] = int(x['post_length'])

        self.creation_date = get_attr(phase, 'creation_date')
        self.phase_status = get_attr(phase, 'phase_status', PHASE_STATUS_NONE)
        self.data_status = get_attr(phase, 'data_status', DATA_STATUS_FRESH)
        self.disabled_posts = get_attr(phase, 'disabled_posts', [])

        self.rows = []

        rows = [m for m in obj if re.match('F#[A-Za-z0-9-_]+#P#[A-Za-z0-9-_]+#R#[A-Za-z0-9-_]+$', m[SORT_KEY])]
        if len(rows) == 0:
            return phase
        # print("Rows: ", rows)
        bays = {}
        for r in sorted(rows, key=lambda x: (int(x['row_number']), x['side'])):
            xr = {}
            if r['side'] not in bays:
                bays[r['side']] = []
            if r['bay'] not in bays[r['side']]:
                bays[r['side']].append(r['bay'])
            __, __, __, __, __, row_key = r[SORT_KEY].split('#')
            xr['row_key'] = row_key
            xr['row_number'] = safe_int(r['row_number'])
            xr['tag_id'] = r['tag_id']
            xr['side'] = r['side']
            xr['bay'] = r['bay']
            xr['row'] = r['row']
            xr['crops'] = r['crops']
            xr['margin_left'] = safe_int(r['margin_left'])
            xr['margin_right'] = safe_int(r['margin_right'])
            xr['max_height'] = safe_int(r['max_height'])
            xr['min_height'] = safe_int(r['min_height'])
            xr['row_length'] = safe_int(r['row_length'])
            xr['row_width'] = safe_int(r['row_width'])
            xr['row_offset'] = safe_int(r['row_offset'])
            xr['rail_distance'] = safe_int(r['rail_distance'])
            xr['is_active'] = r['is_active']
            xr['creation_date'] = r['creation_date']
            self.rows.append(xr)
        
        # for x, v in bays.items():
        #     v.sort()

        self.bays = bays
        self._rows_changed = False

        return self
    
    def find_row(self, tag_id, load_phase = False):
        r = next(iter([x for x in self.rows if x['tag_id'] == tag_id]), None)
        if r:
            return r
        
        # Not found, fetch from DB
        r = self._ph.get_row_by_tag_id(tag_id)        
        if not r:
            return None

        xr = {}
        __, customer_id = r[PARTITION_KEY].split('#')
        __, farm_id, __, phase_id, __, row_key = r[SORT_KEY].split('#')
        xr['customer_id'] = customer_id
        xr['farm_id'] = farm_id
        xr['phase_id'] = phase_id
        xr['row_key'] = row_key
        xr['row_number'] = safe_int(r['row_number'])
        xr['tag_id'] = r['tag_id']
        xr['side'] = r['side']
        xr['bay'] = r['bay']
        xr['row'] = r['row']
        xr['crops'] = r['crops']
        xr['margin_left'] = safe_int(r['margin_left'])
        xr['margin_right'] = safe_int(r['margin_right'])
        xr['max_height'] = safe_int(r['max_height'])
        xr['min_height'] = safe_int(r['min_height'])
        xr['row_length'] = safe_int(r['row_length'])
        xr['row_width'] = safe_int(r['row_width'])
        xr['row_offset'] = safe_int(r['row_offset'])
        xr['rail_distance'] = safe_int(r['rail_distance'])
        xr['is_active'] = r['is_active']
        xr['creation_date'] = r['creation_date']

        # Get the phase object to speed up consequent calls
        if load_phase:
            self.get(customer_id, farm_id, phase_id)

        return xr

    def get_by_id(self, phase_id):
        obj = self._ph.find_phase(phase_id)
        if obj == None or len(obj) == 0:
            raise(ERR_PHASE_NOT_FOUND(f"Phase not found: {phase_id}"))

        phase = obj[0]
        __, customer_id = phase[PARTITION_KEY].split('#')
        __, farm_id, __, phase_id = phase[SORT_KEY].split('#')
        self.get(customer_id, farm_id, phase_id)
        return self

    def new(self,
        customer_id,
        farm_id,
        phase_id="",
        phase_name="",
        phase_type="",
        walkway_width=0,
    ):
        self.customer_id = customer_id
        self.farm_id = farm_id
        self.phase_id = phase_id
        self.phase_name = phase_name
        self.phase_type = phase_type
        self.walkway_width = walkway_width
        self.rows = []
        self.posts = []
        self.bays = {}
        self.disabled_posts = []
        self.creation_date = ""
        self.phase_status = ""
        self.data_status = DATA_STATUS_FRESH
        self._rows_changed = False

        return self

    def save(self):
        if self.customer_id == "" or self.farm_id == "":
            raise(ERR_INVALID_RECORD("Invalid record, no customer_id/farm_id."))

        check_rows = self._rows_changed
        if self.phase_id == "":    
            # New phase, create id
            self.phase_id = str(uuid.uuid4()).lower()
            self.creation_date = str(datetime.now(pytz.utc))
            check_rows = True
        
        # save the phase
        
        if self.phase_status == PHASE_STATUS_NONE or not self.phase_status:
            self.phase_status = PHASE_STATUS_CREATED
        phase = {
            'customer_id': self.customer_id,
            'farm_id': self.farm_id,
            'phase_id': self.phase_id,
            'phase_name': self.phase_name,
            'phase_type': self.phase_type,
            'walkway_width': self.walkway_width,
            'posts': self.posts,
            'disabled_posts': self.disabled_posts,
            'creation_date': self.creation_date,
            'phase_status': self.phase_status,
            'data_status': self.data_status,
        }     

        self._ph.put_customer_phase(
            self.customer_id.lower(),
            self.farm_id.lower(),
            self.phase_id.lower(),
            phase_attr=phase,
            phase_rows_attr=self.rows,
            check_rows=check_rows
        )   
        send_update_message("phase", [self.get_api_dict()], {})
        return self

    def _set_data_status(self, data_status):
        # Used internally, shouldn't be used directly
        if data_status not in DATA_STATUS_VALID_VALUES:
            raise Exception(f'Invalid value: {data_status}')
        self.data_status = data_status
        self._ph.update_customer_phase_data_status(self.customer_id, self.farm_id, self.phase_id, data_status)

    def data_incoming(self):
        from .farm_model import FarmModel
        if DATA_STATUS_INCOMING > self.data_status:
            self._set_data_status(DATA_STATUS_INCOMING)
            farm = FarmModel()
            try:
                farm.get(self.customer_id, self.farm_id)
                farm.data_incoming()
            except:
                pass

    def data_processed(self):
        from .farm_model import FarmModel
        if DATA_STATUS_PRCOESSED > self.data_status:
            self._set_data_status(DATA_STATUS_PRCOESSED)
            farm = FarmModel()
            try:
                farm.get(self.customer_id, self.farm_id)
                farm.data_processed()
            except:
                pass

    def data_archived(self):
        from .farm_model import FarmModel
        if DATA_STATUS_ARCHIVED > self.data_status:
            self._set_data_status(DATA_STATUS_ARCHIVED)
            farm = FarmModel()
            try:
                farm.get(self.customer_id, self.farm_id)
                farm.data_archived()
            except:
                pass

    # This method should be used instead of save() when updating phase, especially when using the Excel file.
    # It compares the object in memory with the version on the database, and according to the phase_status
    #   updates the fields or generates errors if there are inconsistencies.
    def compare_and_update(self):
        target = PhaseModel()
        try:
            target.get(self.customer_id, self.farm_id, self.phase_id)
        except ERR_PHASE_NOT_FOUND:
            target = None
        except Exception as x:
            raise x
        if not target or target.phase_id != self.phase_id:
            # Phase does not exist in db, just create it!
            self.save()
            return

        if target.phase_status != PHASE_STATUS_CREATED:
            self.phase_status = target.phase_status

        if self.phase_status == PHASE_STATUS_CREATED or self.phase_status == PHASE_STATUS_NONE:
            # If we are in create step, just override!
            self.save()
            return 

        if self.phase_status == PHASE_STATUS_ACTIVE:
            # Chack phase attributes
            for a in [m for m in dir(self) if not callable(m) and m in STRUCTURE_PHASE_KEYS]:
                if  getattr(self, a) != getattr(target, a, None):
                    raise Exception(f"Invalid phase definition, field {a}")
            # Check rows attributes
            if len(self.rows) != len(target.rows):
                raise Exception(f"Invalid phase definition, inconsistent number of rows.")
            selfrows = sorted(self.rows, key=lambda x: x["row_key"])
            targetrows = sorted(target.rows, key=lambda x: x["row_key"])
            for i in range(0, len(selfrows)):
                for a in [m for m in dir(selfrows[i]) if not callable(m) and m in STRUCTURE_ROW_KEYS]:
                    if  getattr(selfrows[i], a) != getattr(targetrows[i], a, None):
                        raise Exception(f"Invalid phase definition, row {selfrows[i]['row_number']} side {selfrows[i]['side']}")

            # It seems that we are good to go!            
            self.save()

    def set_status_active(self):
        self.phase_status = PHASE_STATUS_ACTIVE
        self.save()

    def delete(self):
        if self.data_status != DATA_STATUS_FRESH:
            raise ERR_DELETE(f'Delete failed. Phase {self.phase_id} has associated data: {self.data_status}')
        self._ph.del_customer_farm_phase(self.customer_id, self.farm_id, self.phase_id)
        try:
            from .customer_model import CustomerModel
            cst = CustomerModel()
            cst.get(self.customer_id)
            cst.update_message()
        except:
            pass

    def get_cartesian_location(self, row_number, side, distance_cm, height_cm):
        side = side.lower()
        row_key = f"{side}-{row_number}"
        zr = next(iter([x for x in self.rows if x['row_key'] == row_key]), None)
        if not zr:
            raise ERR_INVALID_RECORD("Row not found.")

        if side == 'right':
            x = zr['row_length'] + distance_cm + self.walkway_width
        elif side == 'left':
            x = zr['row_length'] - distance_cm
        else:
            x = distance_cm

        y = (row_number-1) * zr['row_width']
        z = height_cm
        return {
            "x": int(x),
            "y": int(y),
            "z": int(z)
        }

    def get_api_dict(self):
        from .customer_model import CustomerModel
        Customer=CustomerModel()
        try:
            Customer.get(self.customer_id)
            _crm_id = Customer.crm_id
        except:
            _crm_id = "-"
        x = {
           "cust_id": _crm_id,
           "farm_id": str(self.farm_id),
           "phase_id": str(self.phase_id),
           "phase_name": str(self.phase_name),
           "phase_type": str(self.phase_type),
           "walkway_width": int(self.walkway_width),
           "creation_date": str(self.creation_date),
           "is_active": self.is_active,
        }
        bays = [
            {
                "side": c,
                "labels": v
            }
            for c, v in self.bays.items()
        ]
        posts = self.posts
        rows = [
            {
                "side": str(r["side"]),
                "bay": str(r["bay"]),
                "row": str(r["row"]),
                "row_number": int(r["row_number"]),
                "is_active": r["is_active"],
                "crops": [
                    {
                        "side": cl['side'],
                        "crop-variation": cl['variation'],
                        "crop": cl['crop'],
                    }
                    for cl in r["crops"]
                ],
            }
            for r in self.rows
        ]
        disabled_posts = [
            {
                "x": int(dp["x"]),
                "y": int(dp["y"])
            }
            for dp in self.disabled_posts
        ]
        x["bays"] = bays
        x["posts"] = posts
        x["rows"] = rows
        x["disabled_posts"] = disabled_posts
        return x

