from typing import Union, Optional, Dict
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import spark_db, readers, writers, repo_messages
from jobsworthy.util import secrets, fn

ReaderType = Union[readers.DeltaFileReader, readers.DeltaTableReader, readers.HiveTableReader]
StreamReaderType = Union[readers.CosmosStreamReader, readers.DeltaStreamReader]
StreamWriterType = Union[writers.StreamFileWriter, writers.StreamHiveWriter]


def has_method(cls_or_obj, method_name):
    return callable(getattr(cls_or_obj, method_name, None))


def has_non_none_class_attr(obj, attr_name):
    return getattr(obj.__class__, attr_name, None)


class SparkRepo:
    schema = None

    def __init__(self,
                 db: spark_db.Db,
                 reader: Optional[ReaderType] = None,
                 delta_table_reader: Optional[ReaderType] = None,
                 stream_reader: Optional[StreamReaderType] = None,
                 stream_writer: Optional[StreamWriterType] = None,
                 stream_awaiter: Optional[writers.StreamAwaiter] = None,
                 secrets_provider: Optional[secrets.Secrets] = None):
        self.db = db
        self.reader = reader if reader else readers.HiveTableReader
        self.delta_table_reader = delta_table_reader if delta_table_reader else readers.DeltaTableReader
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer
        self.stream_awaiter = stream_awaiter if stream_awaiter else writers.StreamAwaiter
        self.secrets_provider = secrets_provider

    def has_specified_schema(self):
        return (has_method(self, 'schema_') or
                has_non_none_class_attr(self, 'schema') or
                has_method(self, "schema_as_dict"))

    def _struct_schema(self):
        return self.__class__.schema if self.__class__.schema else None

    def determine_schema_to_use_for_df(self, schema_from_argument=None):
        if schema_from_argument:
            return schema_from_argument

        if not self.has_specified_schema():
            raise repo_messages.no_schema_provided_on_create_df()

        return self.schema_as_struct()

    def schema_as_struct(self):
        if not self.has_specified_schema():
            return None
        return self._to_struct_type(self._declared_schema())

    def _to_struct_type(self, sc: Union[T.StructType, Dict]) -> T.StructType:
        if isinstance(sc, T.StructType):
            return sc
        if isinstance(sc, Dict):
            return T.StructType().fromJson(sc)
        return None

    def _declared_schema(self):
        """
        Priority is:
        1. schema found in schema class attr
        2. schema found in schema() function
        3. schema found in schema_as_dict() function.
        """
        if (sc := self.__class__.schema):
            return sc
        if (has_method(self, 'schema_') and (sc := self.schema_()) ):
            return sc
        if (has_method(self, 'schema_as_dict') and (sc := self.schema_as_dict()) ):
            return sc
        return None

    def after_initialise(self):
        ...

    def after_append(self):
        ...

    def after_upsert(self):
        ...

    def after_stream_write_via_delta_upsert(self):
        ...
