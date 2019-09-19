# -*- coding: utf-8 -*-
# Ecoation RawProcessor Registeration
# 
# Created by: Farzad Khandan (farzadkhandan@ecoation.com)
#

from config import SYS_PROXY
import plugins.wave
import plugins.video
import plugins.label
import plugins.aux
import plugins.image
import plugins.model_fruit_count_detail
import plugins.model_fruit_count_summary
import plugins.model_flower_count_detail
import plugins.model_flower_count_summary
import plugins.model_stress_prediction_detail
import plugins.model_stress_prediction_summary


def register_processors():
    SYS_PROXY.register_processor(plugins.wave.RawDataType, plugins.wave.RecordProcessor)
    SYS_PROXY.register_processor(plugins.video.RawDataType, plugins.video.RecordProcessor)
    SYS_PROXY.register_processor(plugins.label.RawDataType, plugins.label.RecordProcessor)
    SYS_PROXY.register_processor(plugins.aux.RawDataType, plugins.aux.RecordProcessor)
    SYS_PROXY.register_processor(plugins.image.RawDataType, plugins.image.RecordProcessor)
    SYS_PROXY.register_processor(plugins.model_fruit_count_detail.RawDataType, plugins.model_fruit_count_detail.RecordProcessor)
    SYS_PROXY.register_processor(plugins.model_fruit_count_summary.RawDataType, plugins.model_fruit_count_summary.RecordProcessor)
    SYS_PROXY.register_processor(plugins.model_flower_count_detail.RawDataType, plugins.model_flower_count_detail.RecordProcessor)
    SYS_PROXY.register_processor(plugins.model_flower_count_summary.RawDataType, plugins.model_flower_count_summary.RecordProcessor)
    SYS_PROXY.register_processor(plugins.model_stress_prediction_detail.RawDataType, plugins.model_stress_prediction_detail.RecordProcessor)
    SYS_PROXY.register_processor(plugins.model_stress_prediction_summary.RawDataType, plugins.model_stress_prediction_summary.RecordProcessor)

