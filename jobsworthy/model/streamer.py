from typing import Callable, Tuple, Dict
from enum import Enum
from uuid import uuid4
from pyspark.sql import dataframe

from jobsworthy.repo import hive_repo
from jobsworthy.util import monad
from . import value, model_errors

class StreamWriterMode(Enum):
    APPEND = 'try_write_stream'
    MERGE = 'stream_write_via_delta_upsert'

class Streamer:
    default_stream_writer_op = StreamWriterMode.APPEND

    def __init__(self,
                 stream_from_table: hive_repo.HiveRepo = None,
                 stream_from_to: hive_repo.HiveRepo = None,
                 transformer: Callable = None,
                 transformer_context: Dict = None,
                 stream_write_op: StreamWriterMode = None):
        self.stream_id = str(uuid4())
        self.runner = Runner()
        self.stream_to_table = stream_from_to
        self.stream_from_table = stream_from_table
        self.transformer = transformer
        self.transformer_context = transformer_context if transformer_context else dict()
        self.stream_write_op = stream_write_op if stream_write_op else self.__class__.default_stream_writer_op

    def stream_from(self, table: hive_repo.HiveRepo):
        self.stream_from_table = table
        return self

    def stream_to(self, table: hive_repo.HiveRepo, partition_columns: Tuple[str] = tuple()):
        self.stream_to_table = table
        self.partition_with = partition_columns
        return self

    def with_write_mode(self, write_mode: StreamWriterMode):
        self.stream_write_op = write_mode
        return self

    def with_transformer(self, transformer: Callable, **kwargs):
        self.transformer = transformer
        self.transformer_context = kwargs
        return self

    def run(self) -> monad.EitherMonad:
        result = self.runner.run(self)
        if result.is_left():
            return monad.Left(result.error)
        return monad.Right(self)


class Runner:

    def run(self, stream):
        return (self.setup_value(stream)
                >> self.stream_initiator
                >> self.apply_transformer
                >> self.start_and_write_stream
                >> self.stream_awaiter)

    def setup_value(self, stream):
        return monad.Right(value.StreamState(stream_configuration=stream))

    def stream_initiator(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        result = val.stream_configuration.stream_from_table.read_stream()

        if not (isinstance(result, dataframe.DataFrame) and result.isStreaming):
            return monad.Left(val.replace('error', model_errors.dataframe_not_streaming()))
        return monad.Right(val.replace('streaming_input_dataframe', result))

    def apply_transformer(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        result = (val.stream_configuration.transformer(val.streaming_input_dataframe,
                                                       **val.stream_configuration.transformer_context))

        if not (isinstance(result, dataframe.DataFrame) and result.isStreaming):
            return monad.Left(val.replace('error', model_errors.dataframe_not_streaming()))
        return monad.Right(val.replace('stream_transformed_dataframe', result))

    def start_and_write_stream(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        result = (getattr(val.stream_configuration.stream_to_table, val.stream_configuration.stream_write_op.value)(
            (val.stream_transformed_dataframe)))
        if result.is_left():
            return monad.Left(val.replace('error', result.error()))
        return monad.Right(val)

    def stream_awaiter(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        val.stream_configuration.stream_to_table.await_termination()
        return monad.Right(val)
