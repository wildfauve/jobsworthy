from typing import Any, Optional
from pyspark.sql import dataframe
from pyspark.sql.streaming import StreamingQuery
from dataclasses import dataclass

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
    error: Optional[Any] = None
