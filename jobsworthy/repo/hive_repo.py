from typing import Optional, Dict, Callable
from enum import Enum
from collections import ChainMap
from functools import partial

from pyspark.sql import dataframe
from pyspark.sql import functions as F
from delta.tables import *
from jobsworthy.util import error, monad, fn, session

from . import spark_db, repo_messages, properties, reader_writer, sql_builder


class Option(Enum):
    MERGE_SCHEMA = ({'mergeSchema': 'true'}, ('spark.databricks.delta.schema.autoMerge.enabled', 'true'))

    @classmethod
    def merge_options(cls, options) -> Dict:
        return dict(ChainMap(*[o.value[0] for o in options]))

    @classmethod
    def options_to_spark_options(cls, options) -> Optional[List[Tuple[str, str]]]:
        """
        Delta tables dont have an options function which would allow setting mergeSchema to true.  The Delta approach
        is to set the spark session conf param 'spark.databricks.delta.schema.autoMerge.enabled' to 'true'.

        This ONLY caters for mergeSchema option.

        self.db.session.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')
        :param session:
        :param options: A collection of Option
        :return:
        """
        return [opt.value[1] for opt in fn.select(lambda option: option.value[1], options)]

    @classmethod
    def options_to_spark_option_names(cls, options) -> Optional[List[Tuple[str, str]]]:
        """
        self.db.session.conf.set('spark.databricks.delta.schema.autoMerge.enabled', 'true')
        :param session:
        :param options: A collection of Option
        :return:
        """
        return [opt.value[1][0] for opt in fn.select(lambda option: option.value[1], options)]


class BaseRepo:
    # Callback Hooks

    def after_initialise(self):
        ...

    def after_append(self):
        ...

    def after_upsert(self):
        ...

    def after_stream_write_via_delta_upsert(self):
        ...


class HiveRepo(BaseRepo):
    default_stream_trigger_condition = {'once': True}

    def __init__(self,
                 db: spark_db.Db,
                 stream_writer=None,
                 reader=reader_writer.HiveTableReader):
        self.db = db
        self.stream_writer = stream_writer
        self.reader = reader
        self.stream_query = None
        self.cached_table_properties = None
        self.stream_upsert_writer = reader_writer.StreamStarter
        if not self.name_of_table():
            raise repo_messages.table_name_not_configured()
        self.property_manager = properties.TablePropertyManager(session=self.db.session,
                                                                asserted_properties=self.asserted_table_properties(),
                                                                db_table_name=self.db_table_name())

        self.properties = self.property_manager  # hides, a little, the class managing properties.

        self.after_initialise()  # callback Hook

    #
    # Table Parameters
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

    def has_specified_schema(self):
        return hasattr(self, "schema_as_dict") or hasattr(self, "schema")

    def table_property_expr(self):
        return properties.TableProperty.table_property_expression(self.asserted_table_properties())

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
        if self.partition_on() and not self.has_specified_schema():
            raise repo_messages.using_partitioning_without_a_create_schema()

        self.db.session.sql(sql_builder.create_unmanaged_table(table_name=self.db_table_name(),
                                                               col_specification=self.sql_column_specification(),
                                                               partition_clause=self.partition_on(),
                                                               table_property_expression=self.table_property_expr(),
                                                               location=self.db.naming().db_table_path(
                                                                   self.table_name)))

        self.property_manager.invalidate_property_cache()

    def create_as_managed_delta_table(self):
        if self.partition_on() and not self.has_specified_schema():
            raise repo_messages.using_partitioning_without_a_create_schema()

        self.db.session.sql(sql_builder.create_managed_table(table_name=self.db_table_name(),
                                                             col_specification=self.sql_column_specification(),
                                                             partition_clause=self.partition_on(),
                                                             table_property_expression=self.table_property_expr()))

        self.property_manager.invalidate_property_cache()

    def column_specification_from_schema(self):
        if not self.has_specified_schema():
            raise repo_messages.no_schema_defined()
        return f"( {self.sql_column_specification()} )"

    def sql_column_specification(self) -> str:
        """
        TODO: Work out a way to suppport nested null types
        Does not currently support applying non-null constraints to nested fields.  This is because the simpleString()
        function on the field does not return the nullable constraint
        :return:
        """
        if not self.has_specified_schema():
            return None
        fields = [self.sql_field_definition(field) for field in self.schema_as_struct()]

        return ", ".join(fields)

    def sql_field_definition(self, field):
        """
        Takes the field and generates a simple string containing name and type.  To convert to a sql specification
        replace the ":" with sp.  Add the NOT NULL expr when  the field is declared non nullable.
        Leve any nested struct untouched.
        :param field:
        :return:
        """
        sql_def = f"{field.simpleString().replace(':', ' ', 1)}"

        # if not isinstance(field.dataType, T.StringType):
        #     sub_type = field.dataType
        #     sub_t_str = sub_type.simpleString()
        #     s1 = sub_t_str[:-1]
        #     s2 = sub_t_str[-1:]
        #     sub_type_def = s1 + " NOT NULL" + s2
        #     sql_def =  sql_def.replace(sub_t_str, sub_type_def)
        #
        # breakpoint()
        if field.nullable:
            return sql_def
        return sql_def + " NOT NULL"

    def determine_schema_to_use_for_df(self, schema_from_argument=None):
        if schema_from_argument:
            return schema_from_argument

        if not self.has_specified_schema():
            raise repo_messages.no_schema_provided_on_create_df()

        return self.schema_as_struct()

    def schema_as_struct(self):
        return self.table_schema() if self.table_schema() else F.StructType().fromJson(self.schema_as_dict())

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
        self.db.session.sql(sql_builder.drop_table(self.db_table_name(dropped_table)))
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
        return reader_writer.DeltaTableReader().read(self, self.table_name)

    def delta_table(self, table_name=None) -> DeltaTable:
        return reader_writer.DeltaTableReader().table(self, table_name if table_name else self.table_name)

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
                                               schema=self.determine_schema_to_use_for_df(schema))

    @monad.monadic_try(error_cls=error.RepoWriteError)
    def try_write_append(self, df, options: Optional[List[Option]] = []):
        return self.write_append(df, options)

    def write_append(self, df, options: Optional[List[Option]] = []):
        """
        Executes a simple append operation on a table using the provided dataframe.
        + Optionally provide a tuple of columns for partitioning the table.
        """
        result = (df.write
                  .format(self.db.table_format())
                  .partitionBy(self.partition_on())
                  .options(**Option.merge_options(options))
                  .mode("append")
                  .saveAsTable(self.db_table_name()))
        self.after_append()  # callback Hook
        return result

    @monad.monadic_try(error_cls=error.RepoWriteError)
    def try_upsert(self, df, options: Optional[List[Option]] = []):
        """
        The try_upsert wraps the upsert function with a Try monad.  The result will be an Either.  A successful result
        usually returns Right(None).
        """
        return self.upsert(df, options)

    def upsert(self, df, options: Optional[List[Option]] = []):
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

        result = self._perform_upsert(df, None, options)

        self.after_upsert()  # Callback

        return result

    def _perform_upsert(self, df, _batch_id=None, options: Optional[List[Option]] = []):
        session.set_session_config_options(self.db.session, Option.options_to_spark_options(options))

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
    def try_write_stream(self, stream, trigger: dict = None, options: Optional[List[Option]] = []):
        return self.write_stream_append(stream=stream,
                                        trigger=trigger,
                                        options=options)

    def write_stream_append(self, stream, trigger: dict = None, options: Optional[List[Option]] = []):
        """
        Write a stream.  Provide a stream.
        + To create partition columns in the table, provide a tuple of column names, the default is no partitioning
        + The default trigger action is {'once': True}. see
          https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.trigger.html
          for more details.
        """
        trigger_condition = trigger if trigger else self.__class__.default_stream_trigger_condition

        self.stream_query = self._write_stream_append_only(stream, self.table_name, trigger_condition, options)
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
        reader_writer.StreamAwaiter().await_termination(self.stream_query)
        df = self.read(self.temp_table_name)
        return df

    @monad.monadic_try(error_cls=error.RepoWriteError)
    def try_stream_write_via_delta_upsert(self,
                                          stream,
                                          trigger: Dict = None,
                                          awaiter: Callable = None,
                                          options: Optional[List[Option]] = []):
        return self.stream_write_via_delta_upsert(stream, trigger, awaiter, options)

    def stream_write_via_delta_upsert(self,
                                      stream,
                                      trigger: Dict = None,
                                      awaiter: reader_writer.StreamAwaiter = None,
                                      options: Optional[List[Option]] = []):

        trigger_condition = trigger if trigger else self.__class__.default_stream_trigger_condition

        self._raise_when_not_in_stream(stream)

        if self.table_exists(self.table_name):
            self.stream_query = self._stream_write_upsert(stream=stream,
                                                          table_name=self.table_name,
                                                          trigger_condition=trigger_condition,
                                                          options=options)
        else:
            if self.is_delta_table_from_path().is_left():
                self.stream_query = self._write_stream_append_only(stream=stream,
                                                                table_name=self.table_name,
                                                                trigger_condition=trigger_condition,
                                                                options=options)
            else:
                self.stream_query = self._stream_write_upsert(stream=stream,
                                                            table_name=self.table_name,
                                                            trigger_condition=trigger_condition,
                                                            options=options)

        if awaiter:
            awaiter().await_termination(stream_query=self.stream_query,
                                        options_for_unsetting=options)

        self.after_stream_write_via_delta_upsert()  # Callback

        return self

    def _raise_when_not_in_stream(self, df):
        if not df.isStreaming:
            raise repo_messages.df_not_in_stream()
        if not self.stream_writer:
            raise repo_messages.writing_stream_without_setting_writer()

    def _write_stream_append_only(self,
                                  stream,
                                  table_name: str,
                                  trigger_condition,
                                  options: Optional[List[Option]] = []):

        self._raise_when_not_in_stream(stream)

        opts = {**Option.merge_options(options),
                **{'checkpointLocation': self.db.naming().checkpoint_location(table_name)}}
        return self.stream_writer().write(self,
                                          stream.writeStream
                                          .format('delta')
                                          .partitionBy(self.partition_on())
                                          .options(**opts)
                                          .trigger(**trigger_condition),
                                          table_name)

    def _stream_write_upsert(self,
                             stream,
                             table_name: str,
                             trigger_condition,
                             options: Optional[List[Option]] = []):

        """
        When wanting to perform a schema merge (based on the option), Delta requires that the spark conf option
        'spark.databricks.delta.schema.autoMerge.enabled' be set to 'true'.  Because of the analysis here
        (https://medium.com/analytics-vidhya/spark-session-and-the-singleton-misconception-1aa0eb06535a) the streaming
        session is cloned from the main spark session.  In the stream this can be obtained from the dataframe using
        df.sparkSession.  But setting the merge option here doesnt appear to work either.  So, we're left with setting
        the autoMerge option on the session BEFORE the session is cloned for the stream.
        :param stream:
        :param table_name:
        :param trigger_condition:
        :param options:
        :return:
        """
        if options:
            session.set_session_config_options(self.db.session, Option.options_to_spark_options(options))

        return (stream.writeStream
                .format('delta')
                .option('checkpointLocation', self.db.naming().checkpoint_location(table_name))
                .trigger(**trigger_condition)
                .foreachBatch(self._perform_upsert)
                .outputMode('append')
                .start())

    def await_termination(self, other_stream_query=None, options_for_unsetting: Optional[List[Option]] = None):
        reader_writer.StreamAwaiter().await_termination(stream_query=self.stream_query,
                                                        other_stream_query=other_stream_query)
        if options_for_unsetting:
            session.unset_session_config_options(self.db.session,
                                                 Option.options_to_spark_option_names(options_for_unsetting))
        return self

    def dont_await(self, _other_stream_query=None):
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


class TemporaryStreamDumpRepo(HiveRepo):
    table_name = "__temp__"
