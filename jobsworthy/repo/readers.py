from __future__ import annotations
from typing import Dict, Protocol, Set
from functools import partial, reduce

from enum import Enum

from pyspark.sql import types as T
from pyspark.sql import dataframe
from delta.tables import *

from . import repo_messages

from jobsworthy.util import fn, monad, error


class ReaderSwitch(Enum):
    READ_STREAM_WITH_SCHEMA_ON = ('read_stream_with_schema', True)  # Read a stream with a schema applied.
    READ_STREAM_WITH_SCHEMA_OFF = ('read_stream_with_schema', False)  # with no schema applied.

    GENERATE_DF_ON = ("generate_df", True)  # for Delta Table reads, return a DF rather than the delta table object
    GENERATE_DF_OFF = ("generate_df", False)  # return a delta table object

    @classmethod
    def merge_options(cls, defaults: Optional[Set], overrides: Optional[Set] = None) -> Set[ReaderSwitch]:
        if overrides is None:
            return defaults

        return reduce(cls.merge_switch, overrides, defaults)

    @classmethod
    def merge_switch(cls, options, override):
        default_with_override = fn.find(partial(cls.option_predicate, override.value[0]), options)
        if not default_with_override:
            options.add(override)
            return options
        options.remove(default_with_override)
        options.add(override)
        return options

    @classmethod
    def option_predicate(cls, option_name, option):
        return option_name == option.value[0]


class ReaderProtocol(Protocol):

    def read(self,
             repo,
             table_name: str,
             reader_options: Optional[Set[ReaderSwitch]]) -> Optional[dataframe.DataFrame]:
        """
        Takes a repository object SparkRepo, and an optional table name and performs a read operation, returning a
        DataFrame.  The result may be optional, especially in the case where the table has yet to be created or
        is not found in the catalogue.
        """
        ...


class DeltaFileReader(ReaderProtocol):
    """
    Reader which reads a delta table from a known table location (table path) using the Spark session.
    """

    def read(self, repo, table_name=None) -> Optional[dataframe.DataFrame]:
        return (repo.db.session.read
                .format('delta')
                .load(repo.delta_table_location(table_name if table_name else repo.table_name)))


class DeltaTableReader(ReaderProtocol):
    """
    Delta reader using the DeltaTable class
    """

    default_reader_options = {ReaderSwitch.GENERATE_DF_ON}

    def read(self,
             repo,
             table_name: str = None,
             reader_options: Optional[Set[ReaderSwitch]] = None) -> Optional[Union[dataframe.DataFrame, DeltaTable]]:
        if not repo.table_exists():
            return None

        table = self._table(repo, table_name)
        if ReaderSwitch.GENERATE_DF_ON in self._merged_options(reader_options):
            return table.toDF()
        return table

    #
    def _table(self, repo, table_name=None) -> DeltaTable:
        return DeltaTable.forPath(repo.db.session,
                                  repo.delta_table_location(table_name if table_name else repo.table_name))

    def _merged_options(self, passed_reader_options: Set[ReaderSwitch] = None) -> Set[ReaderSwitch]:
        if not isinstance(passed_reader_options, set):
            return self.__class__.default_reader_options
        return ReaderSwitch.merge_options(self.__class__.default_reader_options, passed_reader_options)


class HiveTableReader(ReaderProtocol):
    """
    The default table Reader.  Reads data from a Hive database and table location.
    """

    def read(self, repo, table_name=None) -> Optional[dataframe.DataFrame]:
        table_name = table_name if table_name else repo.table_name

        if not repo.table_exists(table_name):
            return None
        return repo.db.session.table(repo.db_table_name(table_name))


class DeltaStreamReader(ReaderProtocol):
    @monad.monadic_try(error_cls=error.RepoStreamReadError)
    def try_read(self,*args, **kwargs):
        return self.read(*args, **kwargs)

    def read(self,
             repo,
             _table_name: str = None,
             reader_options: Optional[Set[ReaderSwitch]] = None) -> Optional[dataframe.DataFrame]:
        opts = set() if not reader_options else reader_options
        return self._read_stream(repo.db.session,
                                 repo.db_table_name(),
                                 ReaderSwitch.READ_STREAM_WITH_SCHEMA_ON in opts,
                                 repo.schema_as_struct())

    def _read_stream(self,
                     session: SparkSession,
                     table_name: str,
                     with_schema: bool,
                     read_schema: T.StructType):
        if with_schema and read_schema:
            return (session
                    .readStream
                    .schema(read_schema)
                    .format('delta')
                    .option('ignoreChanges', True)
                    .table(table_name))

        return (session
                .readStream
                .format('delta')
                .option('ignoreChanges', True)
                .table(table_name))


class CosmosStreamReader(ReaderProtocol):
    format = "cosmos.oltp.changeFeed"

    def read(self,
             repo,
             table_name: str = None,
             reader_options: Optional[Set[ReaderSwitch]] = None) -> Optional[dataframe.DataFrame]:

        try_spark_opts = repo.spark_config_options()

        if try_spark_opts.is_left():
            return None

        return self._read_stream(repo.db.session,
                                 try_spark_opts.value,
                                 self._read_schema_on(reader_options),
                                 repo.schema_as_struct())

    def _read_stream(self,
                     session: SparkSession,
                     spark_options: Dict,
                     with_schema: bool,
                     read_schema: T.StructType):
        stream = (session
                  .readStream.format(self.__class__.format)
                  .options(**spark_options))

        if with_schema and with_schema:
            stream.schema(read_schema)

        return stream.load()

    def _read_schema_on(self, reader_options) -> bool:
        return ReaderSwitch.READ_STREAM_WITH_SCHEMA_ON in self._opts(reader_options)

    def _opts(self, reader_options):
        return set() if not reader_options else reader_options
