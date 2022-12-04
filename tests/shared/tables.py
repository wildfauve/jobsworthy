from pyspark.sql import types as T
from jobsworthy import repo


class MyHiveTable(repo.HiveRepo):
    table_name = "my_hive_table"
    temp_table_name = "_temp_my_hive_table"
    partition_columns = ("name",)
    pruning_column = 'name'

    table_properties = [
        repo.TableProperty(repo.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace")
    ]

    def after_append(self, _result):
        self.property_manager.merge_table_properties()

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"

    def schema_as_dict(self):
        return {'fields': [
            {'metadata': {}, 'name': 'id', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'name', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'pythons', 'nullable': True, 'type': {
                'containsNull': True,
                'elementType': {'fields': [
                    {'metadata': {},
                     'name': 'id',
                     'nullable': True,
                     'type': 'string'}],
                    'type': 'struct'},
                'type': 'array'}},
            {'metadata': {}, 'name': 'season', 'nullable': True, 'type': 'string'}], 'type': 'struct'}


class MyHiveTableWithCallbacks(repo.HiveRepo):
    table_name = "my_hive_table"

    table_properties = [
        repo.TableProperty(repo.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.PARTITION_COLUMNS, "identity", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.PRUNE_COLUMN, "identity", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.PORT, "superTable", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.UPDATE_FREQUENCY, "daily", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.DESCRIPTION, "Some description", "my_namespace"),

    ]

    def after_initialise(self):
        self.create_as_unmanaged_delta_table()

    def schema_as_dict(self):
        return {'fields': [
            {'metadata': {}, 'name': 'id', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'name', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'pythons', 'nullable': True, 'type': {
                'containsNull': True,
                'elementType': {'fields': [
                    {'metadata': {},
                     'name': 'id',
                     'nullable': True,
                     'type': 'string'}],
                    'type': 'struct'},
                'type': 'array'}},
            {'metadata': {}, 'name': 'season', 'nullable': True, 'type': 'string'}], 'type': 'struct'}


class MyHiveTableWithUpdataAndDeleteCondition(repo.HiveRepo):
    table_name = "my_hive_table"
    temp_table_name = "_temp_my_hive_table"
    partition_columns = ("name",)
    pruning_column = 'name'

    table_properties = [
        repo.TableProperty("my_namespace:spark:table:schema:version", "0.0.1")
    ]

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"

    def update_condition(self, _name_of_baseline, update_name):
        return f"{update_name}.isDeleted = false"

    def delete_condition(self, _name_of_baseline, update_name):
        return f"{update_name}.isDeleted = true"



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


class MyHiveTable3(repo.HiveRepo):
    table_name = "my_hive_table_3"

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

def my_table_df_deleted_rows(db):
    return db.session.read.json("tests/fixtures/table1_rows_with_delete.json", multiLine=True, prefersDecimal=True)


def my_table_df_updated_row(db):
    return db.session.read.json("tests/fixtures/table1_rows_3.json", multiLine=True, prefersDecimal=True)
