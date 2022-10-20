from typing import Optional, Dict
from pyspark.sql import dataframe
from delta.tables import *

from . import spark_db
from jobsworth.util import error, monad


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
        if not hasattr(self, "table_name") or not self.__class__.table_name:
            raise error.RepoConfigError('table_name class property not provided')

    def delta_read(self) -> Optional[dataframe.DataFrame]:
        if not self.table_exists():
            return None
        return self.delta_table().toDF()

    def read(self) -> Optional[dataframe.DataFrame]:
        return self.reader().read(self)

    def create_df(self, data, schema=None):
        return self.db.session.createDataFrame(data=data,
                                               schema=self.schema() if self.schema() else schema)

    def schema(self):
        return None

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

    def append(self, df, *partition_cols):
        return self.create(df, *partition_cols)

    def create(self, df, partition_cols: tuple = tuple()):
        return (df.write
                .format(self.db.table_format())
                .partitionBy(partition_cols)
                .mode("append")
                .saveAsTable(self.db_table_name()))

    @monad.monadic_try(error_cls=error.RepoWriteError)
    def try_upsert(self, df, partition_puning_col: str = None, partition_cols: tuple = tuple()):
        return self.upsert(df, partition_puning_col, partition_cols)

    def upsert(self, df, partition_puning_col: str = None, partition_cols: tuple = tuple()):
        if not self.table_exists():
            return self.create(df, partition_cols)

        (self.delta_table().alias(self.table_name)
         .merge(
            df.alias('updates'),
            self.build_merge_condition(self.table_name, 'updates', partition_puning_col)
        )
         .whenNotMatchedInsertAll()
         .execute())

    def build_merge_condition(self, name_of_baseline, update_name, partition_puning_col):
        if not hasattr(self, 'identity_merge_condition'):
            raise error.RepoConfigError(self.error_identity_merge_condition_not_implemented())

        pruning_cond = self.build_puning_condition(name_of_baseline, update_name, partition_puning_col)

        identity_cond = self.identity_merge_condition(name_of_baseline, update_name)

        return f"{pruning_cond}{identity_cond}"

    def build_puning_condition(self, name_of_baseline, update_name, partition_puning_col):
        if partition_puning_col:
            return f"{name_of_baseline}.{partition_puning_col} = {update_name}.{partition_puning_col} AND "
        return ""

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

    def error_identity_merge_condition_not_implemented(self):
        return """
        The repository requires an identity_merge_condition function to perform a delta merge.
        This function takes the name of the baseline and the name of the updates used in the merge.
        Return a delta table condition that contains an identity column name (or sub column name). """
