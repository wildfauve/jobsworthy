from typing import Optional, Set
import pytest

from pyspark.sql import dataframe

from jobsworthy import repo
from jobsworthy.util import error


class MockCosmosDBStreamReader(repo.CosmosStreamReader):
    """
    Overrides the std CosmosStreamReader  _read_stream to remove the cosmos format spark options
    """
    db_table_name = ""

    target_spark_opts = {'spark.cosmos.database', 'spark.cosmos.accountKey', 'spark.cosmos.accountEndpoint',
                         'spark.cosmos.container'}

    def _read_stream(self,
                     session,
                     spark_options,
                     with_schema,
                     read_schema):
        stream = (session
                  .readStream
                  .format("delta")
                  .option("ignoreChanges", True))

        if not isinstance(spark_options, dict) or not self._spark_options_configured_correctly(spark_options):
            raise error.RepoConfigError(f"error in spark options: {spark_options}")

        if with_schema and read_schema:
            stream.schema(read_schema)

        return stream.table(self.__class__.db_table_name)

    def _spark_options_configured_correctly(self, opts):
        return  self.__class__.target_spark_opts.issubset(set(opts.keys()))
