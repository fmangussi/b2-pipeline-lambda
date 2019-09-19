"""
Unit Test for model_stress_prediction_summary module

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
from helpers import UNDEFINED
from plugins.model_stress_prediction_summary.model_stress_prediction_summary import \
    ModelStressPredictionSummaryProcessor
from plugins.label import label_schemas

# scope are: function, class, module, package or session
@pytest.fixture(scope='module')
def processor():
    return ModelStressPredictionSummaryProcessor('ModelStressPredictionSummaryProcessor', config.aws)

COL_CARTESIAN_LOCATION = "cartesian_location"

@contextmanager
def does_not_raise():
    yield


def get_case_1():
    return dict(cartesian_location={"x": 5383, "y": 0, "z": 67},
                     crops=[{"side": "left", "crop": "TOV", "variation": "TOV"},
                            {"side": "right", "crop": "TOV", "variation": "TOV"}],
                     customer_id="c14913a7-3fc1-4764-a856-86fdaa82dcd9", datetime="2019-05-22 11:54:43", # TODO: WHAT IS DATETIMEE?
                     farm_id="19fa24cc-e72b-4f9c-9e7f-d39a93090d7d", label="powdery-mildew",
                     machine_id="89c5d90e129902e4ba83af9d8c74a5e1", phase_id="5eb2eb5f-c509-4a5e-b353-66123bbbbcc3",
                     pressure="2",
                     row_location={"tag_id": "0548207774501389", "row_number": 136, "distance_cm": 3617,
                                   "height_cm": 67,
                                   "post_number": 9, "side": "left", "direction": "right", "velocity": 0},
                     row_number="136", row_session_id="1558551119695", row_side="right", side="left",
                     source="model/powderymildew_tomato_general_hlglr_v1", capture_timestamp="1559894327",
                     upload_timestamp="1559894327")
    # return input_case_1, json_schema_case_1, pytest.raises(fjs.exceptions.JsonSchemaException)


@pytest.mark.parametrize('processor, row_input, json_validator, expectation', [
    (None, get_case_1(), label_schemas, does_not_raise())
], indirect=["processor"])
def test_create_message_payload(processor, row_input, json_validator, expectation):
    with expectation:
        output = next(processor.create_message_payload(row_input))
        print()
        print(output)
        assert json_validator(output) is not None
        assert row_input[COL_CARTESIAN_LOCATION] == output[COL_CARTESIAN_LOCATION]
        assert row_input["customer_id"] == output["customer_id"]
        assert row_input["farm_id"] == output["farm_id"]
        assert row_input["phase_id"] == output["phase_id"]
        assert int(row_input["capture_timestamp"]) == output["capture_timestamp"]
        assert int(row_input["upload_timestamp"]) == output["upload_timestamp"]
        assert row_input["row_session_id"] == output["row_session_id"]
        assert "label" == output["record_type"]
        assert row_input["row_location"] == output["row_location"]
        assert row_input["cartesian_location"] == output["cartesian_location"]
        assert row_input["crops"] == output["crops"]
        assert row_input["machine_id"] == output["machine_id"]
        assert output["hardware_version"] == UNDEFINED
        assert output["firmware_version"] == UNDEFINED
        assert output["version"] == "1.0"
        assert output["capture_local_date"] == output["capture_local_datetime"][:10]
        assert int(row_input["row_number"]) == output["row_location"]["row_number"]
        assert row_input["row_side"] == output["row_location"]["direction"]
        assert row_input["side"] == output["row_location"]["side"]
        assert row_input["row_location"]["distance_cm"] == output["label_meta"]["startDistance"]
        assert row_input["pressure"] == output["label_meta"]["pressure"]
        assert row_input["row_session_id"] == output["label_meta"]["rsid"]
        assert output["capture_local_datetime"] == output["label_meta"]["startTime"]
        assert output["capture_local_datetime"] == output["label_meta"]["endTime"]
        assert output["capture_local_datetime"] == output["label_meta"]["endTime"]
        assert row_input["row_location"]["distance_cm"] == output["label_meta"]["endDistance"]
        assert row_input["label"] == output["label_meta"]["label"]
        assert row_input["source"] == output["label_meta"]["source"]


# "label_meta": {
#     "startDistance": 3617,
#     "pressure": "2",
#     "rsid": "1558551119695",
#     "startTime": "2019-06-07 00:58:47",
#     "endTime": "2019-06-07 00:58:47",
#     "endDistance": 3617,
#     "label": "powdery-mildew",
#     "category": "Disease",
#     "source": "model/powderymildew_tomato_general_hlglr_v1"
#   }





def test_types_to_be_processed(processor):
    assert "model-stress-prediction-summary" in processor.types_to_be_processed()
    assert len(processor.types_to_be_processed()) == 1


def test_invalidate_event():
    pass
