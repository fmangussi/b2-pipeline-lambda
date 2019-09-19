"""
Unit Test for model_stress_prediction_detail module

To convert any schema in json schema, please use the website https://jsonschema.net/ or any other
Schema options:
    Assertions:
        Any:
            Check Infer Enums

To minify the json output, please use the website https://www.webtoolkitonline.com/json-minifier.html or your file editor
"""

from contextlib import contextmanager

import pytest

import config
from plugins.model_stress_prediction_detail.model_stress_prediction_detail import ModelStressPredictionDetailProcessor
from plugins.model_stress_prediction_detail.schema_model_stress_prediction_detail import  schema_model_stress_prediction_detail


@contextmanager
def does_not_raise():
    yield


def get_case_1():
    input_case_1 = dict(
        cartesian_location={
            "x": 5383,
            "y": 0,
            "z": 67,
        }, crops=[
            {
                "side": "left",
                "crop": "TOV",
                "variation": "TOV"
            },
            {
                "side": "right",
                "crop": "TOV",
                "variation": "TOV"
            }
        ], customer_id="c14913a7-3fc1-4764-a856-86fdaa82dcd9", datetime="2019-05-22 11:54:43", distance="3617",
        farm_id="19fa24cc-e72b-4f9c-9e7f-d39a93090d7d", greenhouse_side="left",
        machine_id="89c5d90e129902e4ba83af9d8c74a5e1", phase_id="5eb2eb5f-c509-4a5e-b353-66123bbbbcc3", predictions=[
            {
                "label": "powdery-mildew",
                "model": "powderymildew_tomato_general_hlglr_v1",
                "proba": 0.0008626302974015758
            }
        ], row_location={
            "tag_id": "0548207774501389",
            "row_number": 136,
            "distance_cm": 3617,
            "height_cm": 67,
            "post_number": 0,
            "side": "left",
            "direction": "right",
            "velocity": 0
        }, row_number="136", row_session_id="1558551119695", row_side="right", time="11:54:43",
        capture_timestamp="1559894328", upload_timestamp="1559894328", capture_local_datetime="9999-12-01T11:22:33")

    # json_schema_case_1 = update_enum_property_in_json_schema(input_case_1, json_schema_case_1)

    return input_case_1, schema_model_stress_prediction_detail, does_not_raise()


# def update_enum_property_in_json_schema(input_dict, json_schema):
#     for col_name in input_dict:
#         col_value = input_dict[col_name]
#         json_properties = json_schema["properties"]
#         col_properties = json_properties.get(col_name)
#         if col_properties:
#             acceptable_values = col_properties.get("enum", [])
#             if col_value not in acceptable_values:
#                 acceptable_values.append(col_value)
#             col_properties["enum"] = acceptable_values
#     json_schema = fjs.compile(json_schema)
#     return json_schema

#
# def get_case_2():
#     return 2, does_not_raise()


@pytest.mark.parametrize('row_input, json_validator, expectation', [
    get_case_1()
])
def test_stress_prediction_detail_create_message(row_input, json_validator, expectation):
    with expectation:
        p = ModelStressPredictionDetailProcessor('ModelStressPredictionDetailProcessor', config.aws)
        output = next(p.create_message_payload(row_input))
        assert json_validator(output) is not None

def test_types_to_be_processed():
    p = ModelStressPredictionDetailProcessor('ModelStressPredictionDetailProcessor', config.aws)
    assert "model-stress-prediction-detail" in p.types_to_be_processed() and len(p.types_to_be_processed()) == 1