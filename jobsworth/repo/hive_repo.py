from typing import Optional, Dict
from pyspark.sql import dataframe
from delta.tables import *

from . import spark_db
from jobsworth.util import error


class StreamFileWriter:

    def write(self, repo, stream):
        return stream.start(repo.table_location())


class StreamHiveWriter:
    """
    This is the stream writer to be used on the cluster.  This capability is not supported in local test mode.
    Use StreamFileWriter instead
    """

    def write(self, repo, stream):
        if not hasattr(stream, 'table'):
            raise error.RepoConfigError(
                "StreamHiveWriter can not be used (probably beause in Test), use StreamFileWriter instead")
        return stream.table(repo.db_table_name())


class DeltaFileReader:
    """
    Reader which reads a delta table from a known table location (table path).
    """

    def read(self, repo):
        return repo.db.session.read.format('delta').load(repo.table_location())


class HiveTableReader:
    """
    The default table Reader.  Reads data from a Hive database and table location.
    """

    def read(self, repo):
        if not repo.table_exists():
            return None
        return repo.db.session.table(repo.db_table_name())


class HiveRepo:
    def __init__(self,
                 db: spark_db.Db,
                 stream_writer=None,
                 reader=HiveTableReader):
        self.db = db
        self.stream_writer = stream_writer
        self.reader = reader
        if not hasattr(self, "table_name"):
            raise error.RepoConfigError('table_name class property not provided')

    def delta_read(self) -> Optional[dataframe.DataFrame]:
        if not self.table_exists():
            return None
        return self.delta_table().toDF()

    def read(self) -> Optional[dataframe.DataFrame]:
        return self.reader().read(self)

    def create_df(self, data, schema):
        return self.db.session.createDataFrame(data=data, schema=schema)

    def read_stream(self):
        return (self.db.session
                .readStream
                .format('delta')
                .option('ignoreChanges', True)
                .table(self.db_table_name()))

    def write_stream(self, stream, trigger: dict = None):
        trigger_condition = trigger if trigger else {'once': True}
        if not stream.isStreaming:
            raise error.NotAStreamError("Dataframe is not in a Stream.  Cant write stream")
        self.streamQ = self.stream_writer().write(self, stream
                                                  .writeStream
                                                  .format('delta')
                                                  .option('checkpointLocation',
                                                          self.db.checkpoint_location(self.table_name))
                                                  .trigger(**trigger_condition))
        return self

    def await_termination(self):
        if not self.streamQ:
            return None
        self.streamQ.awaitTermination()

    def create(self, df, *partition_cols):
        (df.write
         .format(self.db.table_format())
         .partitionBy(partition_cols)
         .mode("append")
         .saveAsTable(self.db_table_name()))

    def upsert(self, df, match_col: str, *partition_cols):
        if not self.table_exists():
            return self.create(df, *partition_cols)

        (self.delta_table().alias(self.table_name)
         .merge(
            df.alias('updates'),
            self.build_upsert_match_fn('updates', match_col)
        )
         .whenNotMatchedInsertAll()
         .execute())

    def build_upsert_match_fn(self, update_name: str, match_col: str) -> str:
        return f"{update_name}.{match_col} = {self.table_name}.{match_col}"

    def delta_table(self) -> DeltaTable:
        return DeltaTable.forPath(self.db.session, self.db_table_path())

    def table_exists(self) -> bool:
        return self.db.table_exists(self.table_name)

    def db_table_name(self):
        return self.db.db_table_name(self.table_name)

    def db_table_path(self):
        return f"{self.db.db_path()}/{self.table_name}"

    def table_location(self):
        return self.db.table_location(self.table_name)
