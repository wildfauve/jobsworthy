from pyspark.sql import types as T
from jobsworthy import repo


class MyHiveTable(repo.HiveRepo):
    table_name = "my_hive_table"
    temp_table_name = "_temp_my_hive_table"
    partition_columns = ("name",)
    pruning_column = 'name'

    table_properties = [
        repo.TableProperty("my_namespace:spark:table:schema:version", "0.0.1")
    ]

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"


class MyHiveTableWithoutIdentityCondition(repo.HiveRepo):
    table_name = "my_hive_table"
    temp_table_name = "_temp_my_hive_table"


class MyHiveTableWithoutPartitions(repo.HiveRepo):
    table_name = "my_hive_table"


class MyHiveTable2(repo.HiveRepo):
    table_name = "my_hive_table_2"

    temp_table_name = "_temp_my_hive_table_2"

    partition_columns = ("name",)

    pruning_column = 'name'

    schema = T.StructType(
        [
            T.StructField('id', T.StringType(), True),
            T.StructField('name', T.StringType(), True),
            T.StructField('pythons',
                          T.ArrayType(T.StructType([T.StructField('id', T.StringType(), True)]), True),
                          True),
            T.StructField('season', T.StringType(), True),
            T.StructField('onStream', T.StringType(), False)
        ])

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"


class MyBadlyConfiguredHiveTable(repo.HiveRepo):
    pass


def my_table_df(db):
    return db.session.read.json("tests/fixtures/table1_rows.json", multiLine=True, prefersDecimal=True)


def my_table_df_new_rows(db):
    return db.session.read.json("tests/fixtures/table1_rows_2.json", multiLine=True, prefersDecimal=True)


def my_table_df_updated_row(db):
    return db.session.read.json("tests/fixtures/table1_rows_3.json", multiLine=True, prefersDecimal=True)
