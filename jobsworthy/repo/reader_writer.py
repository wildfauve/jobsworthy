from typing import Optional
import uuid
from pyspark.sql import dataframe, streaming
from delta.tables import *

from jobsworthy.util import monad
from . import repo_messages, hive_repo


class StreamFileWriter:

    def write(self, repo, stream: streaming.DataStreamWriter, table_name=None):
        table_name = table_name if table_name else repo.table_name
        return stream.start(repo.delta_table_location(table_name))


class StreamStarter:

    def write(self, stream: streaming.DataStreamWriter):
        return stream.start()


class StreamHiveWriter:
    """
    This is the stream writer to be used on the cluster.  This capability is not supported in local test mode.
    Use StreamFileWriter instead
    """

    def write(self, repo, stream: streaming.DataStreamWriter, table_name=None):
        if not hasattr(stream, 'table'):
            raise repo_messages.hive_stream_writer_not_available()

        table_name = table_name if table_name else repo.table_name
        return stream.table(repo.db_table_name(table_name))


class DeltaFileReader:
    """
    Reader which reads a delta table from a known table location (table path) using the Spark session.
    """

    def read(self, repo, table_name=None):
        return (repo.db.session.read
                .format('delta')
                .load(repo.delta_table_location(table_name if table_name else repo.table_name)))


class DeltaTableReader:
    """
    Delta reader using the DeltaTable class
    """

    def read(self, repo, table_name=None) -> Optional[dataframe.DataFrame]:
        if not repo.table_exists():
            return None
        return self.table(repo, table_name).toDF()

    #
    def table(self, repo, table_name=None) -> DeltaTable:
        return DeltaTable.forPath(repo.db.session,
                                  repo.delta_table_location(table_name if table_name else repo.table_name))


class HiveTableReader:
    """
    The default table Reader.  Reads data from a Hive database and table location.
    """

    def read(self, repo, table_name=None):
        table_name = table_name if table_name else repo.table_name

        if not repo.table_exists(table_name):
            return None
        return repo.db.session.table(repo.db_table_name(table_name))


class DeltaStreamUpserter:

    def __init__(self, repo, partition_pruning_col: str = None):
        self.repo = repo
        self.partition_pruning_col = partition_pruning_col
        self.temp_id = str(uuid.uuid4()).replace("-", "")
        self.temp_repo = hive_repo.TemporaryStreamDumpRepo(db=self.repo.db,
                                                           reader=DeltaFileReader,
                                                           stream_writer=StreamFileWriter)

    def execute(self, stream) -> monad.EitherMonad[DataFrame]:
        result = (monad.Right(stream)
                  >> self.drop_table_by_name
                  >> self.stream_to_temp
                  >> self.await_termination
                  >> self.read_temp
                  >> self.upsert
                  >> self.drop_table_by_name)
        return result

    def drop_table_by_name(self, stream) -> monad.EitherMonad:
        self.repo.drop_table_by_name(self.temp_table_name(self.temp_repo.table_name))
        return monad.Right(stream)

    def stream_to_temp(self, stream) -> monad.EitherMonad:
        self.stream_query = self.temp_repo.write_stream_to_table(stream,
                                                                 self.temp_table_name(self.temp_repo.table_name),
                                                                 self.repo.__class__.default_stream_trigger_condition)
        return monad.Right(self.stream_query)

    def await_termination(self, stream_query) -> monad.EitherMonad[streaming.StreamingQuery]:
        self.temp_repo.await_termination(stream_query)
        return monad.Right(stream_query)

    def read_temp(self, _stream_query) -> monad.EitherMonad[DataFrame]:
        return monad.Right(self.temp_repo.read(self.temp_table_name(self.temp_repo.table_name)))

    def upsert(self, df: DataFrame) -> monad.EitherMonad[DataFrame]:
        self.repo.upsert(df)
        return monad.Right(df)

    def temp_table_name(self, temp_prefix):
        return f"{temp_prefix}{self.repo.table_name}__{self.temp_id}"


class StreamAwaiter:

    def await_termination(self,
                          stream_query: streaming.StreamingQuery = None,
                          other_stream_query: streaming.StreamingQuery = None,
                          options_for_unsetting=[]):
        target_stream = other_stream_query if other_stream_query else stream_query
        if not target_stream:
            return None

        target_stream.awaitTermination()

        return self
