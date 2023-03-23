from typing import Any, Optional, List
from pyspark.sql import dataframe
from dataclasses import dataclass

from jobsworthy.util.error import JobsWorthyError
from jobsworthy.util import dataklass

@dataclass
class StreamState(dataklass.DataClassAbstract):
    """
    stream_configuration: The model.Streamer object created to configure the stream.
    stream_input_dataframe:  The DF used as input into the stream (the read of the to_table)
    stream_transformed_dataframe:  The DF generated as output from the transformation
    error:  An optional error object subclassed from Exception.
    """
    stream_configuration:  Any
    streaming_input_dataframe: Optional[dataframe.DataFrame] = None
    stream_transformed_dataframe: Optional[dataframe.DataFrame] = None
    error: Optional[JobsWorthyError] = None
