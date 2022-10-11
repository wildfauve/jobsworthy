from typing import Optional
from pyspark.sql import dataframe
from delta.tables import *

from . import spark_db


class HiveRepo:
    def __init__(self, db: spark_db.Db):
        self.db = db
        self.config_root = None

    def delta_read(self) -> Optional[dataframe.DataFrame]:
        if not self.table_exists():
            return None
        return self.delta_table().toDF()

    def read(self):
        if not self.table_exists():
            return None
        return self.db.session.table(self.db_table_name())

    def create_df(self, data, schema):
        return self.db.session.createDataFrame(data=data, schema=schema)

    def read_stream(self):
        return (self.db.session
                .readStream
                .format('delta')
                .option('ignoreChanges', True)
                .table(self.db_table_name()))

    def create(self, df, *partition_cols):
        (df.write
         .format(self.db.table_format())
         .partitionBy(partition_cols)
         .mode("append")
         .saveAsTable(self.db_table_name()))

    def upsert(self, df, *partition_cols):
        if not self.table_exists():
            return self.create(df, *partition_cols)

        # FIXME: generalisation for table
        (self.delta_table().alias('cbor')
         .merge(
            df.alias('updates'),
            'cbor.identity = updates.identity')
         .whenNotMatchedInsertAll()
         .execute())

    def delta_table(self) -> DeltaTable:
        return DeltaTable.forPath(self.db.session, self.db_table_path())

    def table_exists(self) -> bool:
        return self.db.table_exists(self.table_name())

    def db_table_name(self):
        return self.db.db_table_name(self.config_root)

    def table_name(self):
        return self.db.table_name(self.config_root)

    def db_table_path(self):
        return f"{self.db.db_path()}/{self.table_name()}"

