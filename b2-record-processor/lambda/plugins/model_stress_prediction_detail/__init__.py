# Export the implemented raw processor
# you must export these:
# RawDataType: name of the data type that this processor is meant to process
# RawProcessor: the raw processor class (descendent of RawProcessorBase)
RawDataType = 'model_stress_prediction_detail'
from .model_stress_prediction_detail import ModelStressPredictionDetailProcessor as RecordProcessor
