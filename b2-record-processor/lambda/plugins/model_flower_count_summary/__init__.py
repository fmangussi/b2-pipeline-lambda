# Export the implemented raw processor
# you must export these:
# RawDataType: name of the data type that this processor is meant to process
# RawProcessor: the raw processor class (descendent of RawProcessorBase)
from .model_flower_count_summary import ModelFlowerCountSummaryProcessor as RecordProcessor
RawDataType = 'model_flower_count_summary'
