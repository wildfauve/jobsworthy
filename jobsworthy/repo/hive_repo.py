import uuid
from typing import Optional, Dict

import pyspark.sql.streaming
from pyspark.sql import dataframe
from pyspark.sql import functions as F
from delta.tables import *
from pymonad.tools import curry

from . import spark_db, repo_messages
from jobsworthy.util import error, monad


class TableProperty:
    @classmethod
    def table_property_expression(cls, set_of_props: List):
        return ",".join([prop.format_as_expression() for prop in set_of_props])

    @classmethod
    def table_property_expression_keys(cls, set_of_props: List):
        return ",".join([prop.format_key_as_expression for prop in set_of_props])

    def __init__(self, key: str, value: str):
        self.key = self.prepend_urn(key)
        self.value = value

    def prepend_urn(self, key):
        if key[0:3] == 'urn':
            return key
        return f"urn:{key}"

    def __key(self):
        return (self.key, self.value)

    def __hash__(self):
        return hash((self.key, self.value))

    def __eq__(self, other):
        return self.__key() == other.__key()

    def format_as_expression(self):
        return f"'{self.key}'='{self.value}'"

    def format_key_as_expression(self):
        return f"'{self.key}'"


class StreamFileWriter:

    def write(self, repo, stream, table_name=None):
        table_name = table_name if table_name else repo.table_name
        return stream.start(repo.delta_table_location(table_name))


class StreamHiveWriter:
    """
    This is the stream writer to be used on the cluster.  This capability is not supported in local test mode.
    Use StreamFileWriter instead
    """

    def write(self, repo, stream, table_name=None):
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


class HiveRepo:
    default_stream_trigger_condition = {'once': True}

    def __init__(self,
                 db: spark_db.Db,
                 stream_writer=None,
                 reader=HiveTableReader):
        self.db = db
        self.stream_writer = stream_writer
        self.reader = reader
        self.stream_query = None
        if not self.name_of_table():
            raise repo_messages.table_name_not_configured()

    #
    # Table Properties
    #
    def partition_on(self):
        return self.__class__.partition_columns if hasattr(self, 'partition_columns') else tuple()

    def name_of_table(self):
        return self.__class__.table_name if hasattr(self, "table_name") else None

    def table_schema(self):
        return self.__class__.schema if hasattr(self, "schema") else None

    def prune_on(self):
        return self.__class__.pruning_column if hasattr(self, 'pruning_column') else None

    def associated_temporary_table(self):
        return self.__class__.temp_table_name if hasattr(self, "temp_table_name") else None

    def merge_condition(self):
        return self.identity_merge_condition if hasattr(self, 'identity_merge_condition') else None

    def asserted_table_properties(self):
        return self.__class__.table_properties if hasattr(self, 'table_properties') else None

    #
    # Table Lifecycle Events
    #
    def table_exists(self, table_name=None) -> bool:
        table_name = table_name if table_name else self.table_name
        return self.db.table_exists(table_name)

    def is_delta_table_from_path(self) -> monad.EitherMonad:
        """
        Useful when the table is not registered with HIVE.  Attempts to create a DF from a delta path.
        Right means the delta table exists, Left means it doesn't.
        :param repo:
        :param table_name:
        :return:
        """
        return monad.monadic_try()(self.delta_table)()

    def create_as_unmanaged_delta_table(self):
        return self.db.session.sql(f"""
        CREATE TABLE {self.table_name}
        USING DELTA 
        LOCATION '{self.db.naming().db_table_path(self.table_name)}'
        """)

    def drop_table(self):
        """
        Drops the table if it exists.  Use carefully!
        :return:
        """
        return self.drop_table_by_name()

    def drop_table_by_name(self, table_to_drop: str = None):
        """
        By default drops the main table representing the repo (that is self.table_name).
        However, when dropping the temporary table (or other associated tables), provide the
        table name.
        :return:
        """
        dropped_table = table_to_drop if table_to_drop else self.table_name
        self.db.session.sql(f"DROP TABLE IF EXISTS {self.db_table_name(dropped_table)}")
        return self

    def drop_temp_table(self):
        if not self.associated_temporary_table():
            raise repo_messages.temp_table_not_configured()

        return self.drop_table_by_name(table_to_drop=self.associated_temporary_table())

    #
    # Table Read Functions
    #

    def delta_read(self) -> Optional[dataframe.DataFrame]:
        if not self.table_exists():
            return None
        return DeltaTableReader().read(self, self.table_name)

    def delta_table(self, table_name=None) -> DeltaTable:
        return DeltaTableReader().table(self, table_name if table_name else self.table_name)

    def read(self, target_table_name: str = None) -> Optional[dataframe.DataFrame]:
        return self.reader().read(self, target_table_name)

    def read_stream(self) -> DataFrame:
        return (self.db.session
                .readStream
                .format('delta')
                .option('ignoreChanges', True)
                .table(self.db_table_name()))

    #
    # Table Write Functions
    #
    def create_df(self, data, schema=None):
        return self.db.session.createDataFrame(data=data,
                                               schema=self.determine_schema_to_use(schema))

    def determine_schema_to_use(self, schema_from_argument=None):
        if schema_from_argument:
            return schema_from_argument
        if self.table_schema():
            return self.table_schema()
        raise repo_messages.no_schema_provided_on_create_df()

    @monad.monadic_try(error_cls=error.RepoWriteError)
    def try_write_append(self, df):
        return self.write_append(df)

    def write_append(self, df):
        """
        Executes a simple append operation on a table using the provided dataframe.
        + Optionally provide a tuple of columns for partitioning the table.
        """
        result = (df.write
                  .format(self.db.table_format())
                  .partitionBy(self.partition_on())
                  .mode("append")
                  .saveAsTable(self.db_table_name()))
        self.merge_table_properties()
        return result

    @monad.monadic_try(error_cls=error.RepoWriteError)
    def try_upsert(self, df):
        """
        The try_upsert wraps the upsert function with a Try monad.  The result will be an Either.  A successful result
        usually returns Right(None).
        """
        return self.upsert(df)

    def upsert(self, df):
        """
        Upsert performs either a create or a delta merge function.  The create is called when the table doesnt exist.
        Otherwise a delta merge is performed.
        + partition_pruning_col.  When partitioning, the merge will use one of the partition columns to execute a merge using
          partition pruning.
        + Optionally provide partition columns as a tuple of column names.

        Note that the merge requires that the repository implement a identity_merge_condition function when using merge
        operations.  This is a merge condition that identifies the upsert identity.
        """
        if not self.table_exists():
            return self.write_append(df)

        return self._perform_upsert(df, None)

    def _perform_upsert(self, df, _batch_id=None):
        upserter = (self.delta_table().alias(self.table_name)
                    .merge(df.alias('updates'),
                           self.build_merge_condition(self.table_name, 'updates', self.prune_on()))
                    .whenNotMatchedInsertAll())

        # note that the conditions appear to return a new version of te upserter, rather than mutating the merge
        # class.  Therefore, to apply the UpdateAll and Delete conditionally, the upserter var needs to be updated with
        # the new version of the merge class.
        # TODO: a better approach might be a reduce(condition_applier, [whenMatchedUpdateAll, whenMatchedDelete], upserter)
        if hasattr(self, 'update_condition'):
            upserter = upserter.whenMatchedUpdateAll(condition=self.update_condition(self.table_name, 'updates'))
        else:
            upserter = upserter.whenMatchedUpdateAll()

        if hasattr(self, 'delete_condition'):
            upserter = upserter.whenMatchedDelete(condition=self.delete_condition(self.table_name, 'updates'))

        return upserter.execute()

    #
    # Streaming Functions
    #
    @monad.monadic_try(error_cls=error.RepoWriteError)
    def try_write_stream(self, stream, trigger: dict = None):
        return self.write_stream_append(stream, trigger)

    def write_stream_append(self, stream, trigger: dict = None):
        """
        Write a stream.  Provide a stream.
        + To create partition columns in the table, provide a tuple of column names, the default is no partitioning
        + The default trigger action is {'once': True}. see
          https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.trigger.html
          for more details.
        """
        trigger_condition = trigger if trigger else self.__class__.default_stream_trigger_condition

        self.stream_query = self._write_stream_append_only(stream, self.table_name, trigger_condition)
        return self

    def write_stream_temporary(self, stream) -> DataFrame:
        if not self.associated_temporary_table():
            raise repo_messages.temp_table_not_configured()

        """
        Write a stream to dataframe.  Provide a stream.
        + Always uses the default trigger action {'once': True}. see
          https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.trigger.html
          for more details.
        """
        trigger_condition = self.__class__.default_stream_trigger_condition

        self.drop_temp_table()

        self.stream_query = self._write_stream_append_only(stream,
                                                           self.associated_temporary_table(),
                                                           trigger_condition)
        self.await_termination()
        df = self.read(self.temp_table_name)
        return df

    # def delta_stream_upserter(self,
    #                           stream: dataframe.DataFrame,
    #                           partition_pruning_col: str = None,
    #                           partition_cols: tuple = tuple()):
    #     return DeltaStreamUpserter(self, partition_pruning_col, partition_cols).execute(stream)

    def stream_write_via_delta_upsert(self,
                                      stream,
                                      trigger: Dict = None):

        trigger_condition = trigger if trigger else self.__class__.default_stream_trigger_condition

        self._raise_when_not_in_stream(stream)

        if self.is_delta_table_from_path().is_left():
            self.stream_query = self._write_stream_append_only(stream, self.table_name, trigger_condition)
        else:
            self.stream_query = self._stream_write_upsert(stream, self.table_name, trigger_condition)
        return self

    def _raise_when_not_in_stream(self, df):
        if not df.isStreaming:
            raise repo_messages.df_not_in_stream()
        if not self.stream_writer:
            raise repo_messages.writing_stream_without_setting_writer()

    def _write_stream_append_only(self,
                                  stream,
                                  table_name: str,
                                  trigger_condition):

        self._raise_when_not_in_stream(stream)

        return self.stream_writer().write(self,
                                          stream.writeStream
                                          .format('delta')
                                          .partitionBy(self.partition_on())
                                          .option('checkpointLocation',
                                                  self.db.naming().checkpoint_location(table_name))
                                          .trigger(**trigger_condition),
                                          table_name)

    def _stream_write_upsert(self,
                             stream,
                             table_name: str,
                             trigger_condition):
        return self.stream_writer().write(self,
                                          stream.writeStream
                                          .format('delta')
                                          .option('checkpointLocation',
                                                  self.db.naming().checkpoint_location(table_name))
                                          .trigger(**trigger_condition)
                                          .foreachBatch(self._perform_upsert)
                                          .outputMode('append'),
                                          table_name)

    def await_termination(self, other_stream_query=None):
        target_stream = other_stream_query if other_stream_query else self.stream_query
        if not target_stream:
            return None

        target_stream.awaitTermination()
        return self

    #
    # Utility Functions
    #
    def build_merge_condition(self, name_of_baseline, update_name, partition_pruning_col):
        if not self.merge_condition():
            raise repo_messages.error_identity_merge_condition_not_implemented()

        pruning_cond = self.build_puning_condition(name_of_baseline, update_name, partition_pruning_col)

        identity_cond = self.merge_condition()(name_of_baseline, update_name)

        return f"{pruning_cond}{identity_cond}"

    def build_puning_condition(self, name_of_baseline, update_name, partition_puning_col):
        if partition_puning_col:
            return f"{name_of_baseline}.{partition_puning_col} = {update_name}.{partition_puning_col} AND "
        return ""

    def build_upsert_match_fn(self, update_name: str, match_col: str) -> str:
        return f"{update_name}.{match_col} = {self.table_name}.{match_col}"

    #
    # Table Naming Functions
    #
    def db_table_name(self, table_name=None):
        return self.db.naming().db_table_name(table_name if table_name else self.table_name)

    def db_temp_table_name(self):
        return self.db.naming().db_table_name(self.temp_table_name)

    def db_table_path(self, table_name=None):
        return self.db.naming().db_table_path(table_name if table_name else self.table_name)

    def delta_table_location(self, table_name=None):
        if not self.db.naming().delta_table_naming_correctly_configured():
            raise repo_messages.delta_location_configured_incorrectly()
        return self.db.naming().delta_table_location(table_name if table_name else self.table_name)

    #
    # Table Property Functions
    #
    def merge_table_properties(self):
        if not self.asserted_table_properties():
            return self
        set_on_table = set(self.urn_table_properties())

        self.add_to_table_properties(set(self.asserted_table_properties()) - set_on_table)
        self.remove_from_table_properties(set_on_table - set(self.asserted_table_properties()))
        return self

    def urn_table_properties(self) -> List[TableProperty]:
        return [TableProperty(prop.key, prop.value) for prop in (self.get_table_properties()
                                                                 .filter(F.col('key').startswith('urn'))
                                                                 .select(F.col('key'), F.col('value'))
                                                                 .collect())]

    def get_table_properties(self):
        return self.db.session.sql(f"SHOW TBLPROPERTIES {self.db_table_name()}")

    def add_to_table_properties(self, to_add: List[TableProperty]):
        if not to_add:
            return self
        self.db.session.sql(
            f"alter table {self.db_table_name()} set tblproperties({TableProperty.table_property_expression(to_add)})")
        return self

    def remove_from_table_properties(self, to_remove: List[TableProperty]):
        if not to_remove:
            return self
        self.db.session.sql(
            f"alter table {self.db_table_name()} unset tblproperties({TableProperty.table_property_expression_keys(to_remove)})")
        return self

    # def __del__(self):
    #     if hasattr(self, "temp_table_name") or self.__class__.temp_table_name:
    #         self.clear_temp_storage()


class DeltaStreamUpserter:
    class TemporaryStreamDumpRepo(HiveRepo):
        table_name = "__temp__"

    def __init__(self, repo: HiveRepo, partition_pruning_col: str = None):
        self.repo = repo
        self.partition_pruning_col = partition_pruning_col
        self.temp_id = str(uuid.uuid4()).replace("-", "")
        self.temp_repo = self.TemporaryStreamDumpRepo(db=self.repo.db,
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

    def await_termination(self, stream_query) -> monad.EitherMonad[pyspark.sql.streaming.StreamingQuery]:
        self.temp_repo.await_termination(stream_query)
        return monad.Right(stream_query)

    def read_temp(self, _stream_query) -> monad.EitherMonad[DataFrame]:
        return monad.Right(self.temp_repo.read(self.temp_table_name(self.temp_repo.table_name)))

    def upsert(self, df: DataFrame) -> monad.EitherMonad[DataFrame]:
        self.repo.upsert(df)
        return monad.Right(df)

    def temp_table_name(self, temp_prefix):
        return f"{temp_prefix}{self.repo.table_name}__{self.temp_id}"
