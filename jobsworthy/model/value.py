from typing import Any, Optional, List
from pyspark.sql import dataframe
from dataclasses import dataclass

from jobsworthy.util.error import JobError

@dataclass
class DataClassAbstract:
    def replace(self, key, value):
        setattr(self, key, value)
        return self


@dataclass
class StreamState(DataClassAbstract):
    stream_configuration:  Any
    streaming_input_dataframe: Optional[dataframe.DataFrame] = None
    stream_transformed_dataframe: Optional[dataframe.DataFrame] = None
    error: Optional[JobError] = None
