from typing import Optional
import pytest


class MockCosmosDBStreamReader:
    db_table_name = ""

    def read_stream(self, session, spark_options, read_schema: Optional = None):
        stream = (session
                  .readStream
                  .format("delta")
                  .option("ignoreChanges", True))

        if read_schema:
            stream.schema(read_schema)

        return stream.table(self.__class__.db_table_name)
