# Export the implemented raw processor
# you must export these:
# RawDataType: name of the data type that this processor is meant to process
# RawProcessor: the raw processor class (descendent of RawProcessorBase)
from .label_pro import LabelRecordProcessor as RecordProcessor
RawDataType = 'label'
