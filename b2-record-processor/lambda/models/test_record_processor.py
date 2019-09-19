# -*- coding: utf-8 -*-
#
# @author: Fernando Mangussi <fernando@ecoation.com>
#

import pytest

from helpers import UNDEFINED


def test_get_filename_info_exception(record_processor_instance, wave_valid_event):
    rp = record_processor_instance
    rp.set_event_message(wave_valid_event)
    rp._file_match = ""
    rp.stream_message._dict['filename'] = 'pytest'
    assert rp.get_filename_info()["file_type"] == UNDEFINED


def test_get_filename_info_filename_v1(record_processor_instance, wave_valid_event):
    rp = record_processor_instance
    rp.set_event_message(wave_valid_event)
    filename = 'unittestv1-file_type/unittestv1-machine_id/unv1-te-st/unittestv1_tag_id-unittestv1_timestamp-unittestv1_extra_info-unitv1.unittestv1_extension'
    rp._file_match = None
    rp.stream_message._dict['filename'] = filename

    result = rp.get_filename_info()

    expected_result = dict(
        file_type='unittestv1-file_type'
        , machine_id='unittestv1-machine_id'
        , upload_date='unv1-te-st'
        , tag_id='unittestv1_tag_id'
        , rsid=UNDEFINED
        , timestamp='unittestv1_timestamp'
        , extra_info='unittestv1_extra_info'
        , identifier='unitv1'
        , fileextension='unittestv1_extension'
        , phase_id=UNDEFINED
    )
    assert result == expected_result

def test_get_filename_info_filename_v2(record_processor_instance, wave_valid_event):
    rp = record_processor_instance
    rp.set_event_message(wave_valid_event)
    filename = 'unittestv2-file_type/unittestv2-machine_id/unv2-te-st/unittestv2_tag_id-unittestv2_rsid-unittestv2_timestamp-unittestv2_extra_info-unitv2.unittestv2_extension'
    rp._file_match = None
    rp.stream_message._dict['filename'] = filename

    result = rp.get_filename_info()

    expected_result = dict(
        file_type='unittestv2-file_type'
        , machine_id='unittestv2-machine_id'
        , upload_date='unv2-te-st'
        , tag_id='unittestv2_tag_id'
        , rsid='unittestv2_rsid'
        , timestamp='unittestv2_timestamp'
        , extra_info='unittestv2_extra_info'
        , identifier='unitv2'
        , fileextension='unittestv2_extension'
        , phase_id=UNDEFINED
    )
    assert result == expected_result

def test_get_filename_info_filename_v3(record_processor_instance, wave_valid_event):
    rp = record_processor_instance
    rp.set_event_message(wave_valid_event)
    filename = 'unittestv3_basefolder_level1/unittestv3_basefolder_level2/unittestv3_filetype/unittestv3_tag_id-unittestv3_rsid-unittestv3_timestamp-unittestv3_extra_info-unitv3.unittestv3_extension'
    rp._file_match = None
    rp.stream_message._dict['filename'] = filename

    result = rp.get_filename_info()

    expected_result = dict(
        file_type='unittestv3_filetype'
        , machine_id=UNDEFINED
        , upload_date=UNDEFINED
        , tag_id='unittestv3_tag_id'
        , rsid='unittestv3_rsid'
        , timestamp='unittestv3_timestamp'
        , extra_info='unittestv3_extra_info'
        , identifier='unitv3'
        , fileextension='unittestv3_extension'
        , phase_id=UNDEFINED
    )
    assert result == expected_result

def test_open_stage_file_exception(record_processor_instance):
    rp = record_processor_instance
    with pytest.raises(ValueError):
        assert rp.open_stage_file('unittest_test_open_stage_file_exception') is None


# def test_log_error_exception(record_processor_instance):
#     rp = record_processor_instance
#     redirected_output = sys.stdout = StringIO()
#     redirected_error = sys.stderr = StringIO()
#     rp.log_error(method=None, arguments=None, error_message=None)
#     assert len(redirected_error) > 0


"""
lambda/models/record_processor.py
 246-248,
 251-254,
 313-329,
 336,
 346,
 359-360,
 366-368,
 416,
 418-419,
 440-446,
 453-455,
 490,
 519,
 540-541,
 556,
 558-559,
 569,
 584,
 593-595,
 622,
 658-661,
 722-723,
 726,
 729-732,
 735,
 748-750,
 753,
 768,
 774,
 786,
 792-798
"""
