from typing import Callable, Tuple, Dict, List, Optional
from enum import Enum
from uuid import uuid4
from pyspark.sql import dataframe

from jobsworthy import repo
from jobsworthy.util import monad
from . import value, model_errors


class StreamWriteType(Enum):
    APPEND = "try_write_stream"
    UPSERT = "try_stream_write_via_delta_upsert"


class StreamToPair:

    def __init__(self):
        self.stream_to_table = None
        self.transformer = None
        self.transformer_context = None
        self.transformed_df = None
        self.stream_write_type = None
        self.stream_write_options = []

    def stream_to(self,
                  table: repo.HiveRepo,
                  write_type: StreamWriteType = StreamWriteType.APPEND,
                  options: Optional[List[repo.Option]] = [],
                  stream_trigger_condition: Optional[Dict] = None):
        self.stream_to_table = table
        self.stream_write_type = write_type
        self.stream_write_options = options
        if stream_trigger_condition:
            self.stream_trigger_condition = stream_trigger_condition
        else:
            self.stream_trigger_condition = repo.HiveRepo.default_stream_trigger_condition
        return self

    def with_transformer(self, transformer: Callable, **kwargs):
        self.transformer = transformer
        self.transformer_context = kwargs
        return self

    def apply_transformer(self, input_df):
        self.transformed_df = self.transformer(input_df, **self.transformer_context)

        if not (isinstance(self.transformed_df, dataframe.DataFrame) and self.transformed_df.isStreaming):
            return monad.Left(model_errors.dataframe_not_streaming())
        return monad.Right(self.transformed_df)

    def run_stream(self):
        """
        Invokes the repo function to start and run the stream, providing the transformation df as an input.
        The stream_write_type enum value provides the streaming write type to call on the repo, either an append
        or an upsert.

        :return:
        """
        return (getattr(self.stream_to_table, self.stream_write_type.value)
                (stream=self.transformed_df,
                 trigger=self.stream_trigger_condition,
                 options=self.stream_write_options))

    def await_termination(self):
        return self.stream_to_table.await_termination(options_for_unsetting=self.stream_write_options)


class MultiStreamer:
    def __init__(self,
                 stream_from_table: repo.HiveRepo = None):
        self.stream_id = str(uuid4())
        self.stream_from_table = stream_from_table
        self.runner = Runner()
        self.stream_pairs = []
        self.multi = True

    def stream_from(self, table: repo.HiveRepo):
        self.stream_from_table = table
        return self

    def with_stream_to_pair(self, stream_to_pair: StreamToPair):
        self.stream_pairs.append(stream_to_pair)
        return self

    def run(self) -> monad.EitherMonad:
        result = self.runner.run(self)
        if result.is_left():
            breakpoint()
            return monad.Left(result.error)
        return monad.Right(self)


class Streamer:

    def __init__(self,
                 stream_from_table: repo.HiveRepo = None,
                 stream_from_to: repo.HiveRepo = None,
                 transformer: Callable = None,
                 transformer_context: Dict = None,
                 partition_with: Tuple = None):
        self.stream_id = str(uuid4())
        self.runner = Runner()
        self.stream_to_table = stream_from_to
        self.stream_from_table = stream_from_table
        self.transformer = transformer
        self.transformer_context = transformer_context if transformer_context else dict()
        self.stream_write_type = None
        self.stream_write_options = []
        self.multi = False

    def stream_from(self, table: repo.HiveRepo):
        self.stream_from_table = table
        return self

    def stream_to(self,
                  table: repo.HiveRepo,
                  partition_columns: Tuple[str] = tuple(),
                  write_type: StreamWriteType = StreamWriteType.APPEND,
                  options: Optional[List[repo.Option]] = [],
                  stream_trigger_condition: Optional[Dict] = None):
        self.stream_to_table = table
        self.partition_with = partition_columns
        self.stream_write_type = write_type
        self.stream_write_options = options
        if stream_trigger_condition:
            self.stream_trigger_condition = stream_trigger_condition
        else:
            self.stream_trigger_condition = repo.HiveRepo.default_stream_trigger_condition
        return self

    def with_transformer(self, transformer: Callable, **kwargs):
        self.transformer = transformer
        self.transformer_context = kwargs
        return self

    def run(self) -> monad.EitherMonad:
        result = self.runner.run(self)
        if result.is_left():
            return monad.Left(result.error())
        return monad.Right(self)


class Runner:

    def run(self, stream):
        return (self.setup_value(stream)
                >> self.stream_initiator
                >> self.transformer_strategy
                >> self.start_stream_strategy
                >> self.stream_awaiter_strategy)

    def setup_value(self, stream):
        return monad.Right(value.StreamState(stream_configuration=stream))

    def stream_initiator(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        result = val.stream_configuration.stream_from_table.read_stream()

        if not (isinstance(result, dataframe.DataFrame) and result.isStreaming):
            return monad.Left(val.replace('error', model_errors.dataframe_not_streaming()))
        return monad.Right(val.replace('streaming_input_dataframe', result))

    def transformer_strategy(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        if val.stream_configuration.multi:
            return self.apply_multi_transformers(val)
        return self.apply_transformer(val)

    def apply_multi_transformers(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        results = [pair.apply_transformer(val.streaming_input_dataframe) for pair in
                   val.stream_configuration.stream_pairs]

        if not all(map(monad.maybe_value_ok, results)):
            monad.Left(val.replace('error', model_errors.dataframe_not_streaming()))
        return monad.Right(val)

    def apply_transformer(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        result = (val.stream_configuration.transformer(val.streaming_input_dataframe,
                                                       **val.stream_configuration.transformer_context))

        if not (isinstance(result, dataframe.DataFrame) and result.isStreaming):
            return monad.Left(val.replace('error', model_errors.dataframe_not_streaming()))
        return monad.Right(val.replace('stream_transformed_dataframe', result))

    def start_stream_strategy(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        if val.stream_configuration.multi:
            return self.start_and_run_multi_streams(val)
        return self.start_and_run_stream(val)

    def start_and_run_multi_streams(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        results = [pair.run_stream() for pair in val.stream_configuration.stream_pairs]

        if not all(map(monad.maybe_value_ok, results)):
            monad.Left(val.replace('error', monad.Left("Boom!")))
        return monad.Right(val)

    def start_and_run_stream(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        """
        Invokes the repo function to start and run the stream, providing the transformation df as an input.
        The stream_write_type enum value provides the streaming write type to call on the repo, either an append
        or an upsert.

        :param val:
        :return:
        """
        result = (getattr(val.stream_configuration.stream_to_table, val.stream_configuration.stream_write_type.value)
                  (stream=val.stream_transformed_dataframe,
                   trigger=val.stream_configuration.stream_trigger_condition,
                   options=val.stream_configuration.stream_write_options))

        if result.is_left():
            return monad.Left(val.replace('error', result.error()))
        return monad.Right(val)

    def stream_awaiter_strategy(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        if val.stream_configuration.multi:
            return self.multi_stream_awaiter(val)
        return self.stream_awaiter(val)

    def multi_stream_awaiter(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        [pair.await_termination() for pair in val.stream_configuration.stream_pairs]
        return monad.Right(val)

    def stream_awaiter(self, val: value.StreamState) -> monad.EitherMonad[value.StreamState]:
        val.stream_configuration.stream_to_table.await_termination(
            options_for_unsetting=val.stream_configuration.stream_write_options)
        return monad.Right(val)
