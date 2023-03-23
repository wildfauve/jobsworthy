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

    def after_append(self):
        self.properties.merge_table_properties()

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"

    def schema_(self):
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


class MyHiveTableCreatedAsManagedTable(repo.HiveRepo):
    table_creation_protocol = repo.CreateManagedDeltaTableSQL

    table_name = "my_hive_table_created_as_managed_table"

    table_properties = [
        repo.TableProperty(repo.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.PARTITION_COLUMNS, "identity", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.PRUNE_COLUMN, "identity", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.PORT, "superTable", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.UPDATE_FREQUENCY, "daily", "my_namespace"),
        repo.TableProperty(repo.DataAgreementType.DESCRIPTION, "Some description", "my_namespace"),

    ]

    def after_initialise(self):
        self.perform_table_creation_protocol()
        pass

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"

    def schema_(self):
        return {'type': 'struct',
                'fields': [{'name': 'id', 'type': 'string', 'nullable': True, 'metadata': {}},
                           # {'name': 'isDeleted', 'type': 'string', 'nullable': True, 'metadata': {}},
                           {'name': 'name', 'type': 'string', 'nullable': True, 'metadata': {}},
                           {'name': 'pythons', 'type': {'type': 'array',
                                                        'elementType': {'type': 'struct', 'fields': [
                                                            {'name': 'id', 'type': 'string',
                                                             'nullable': True, 'metadata': {}}]},
                                                        'containsNull': True}, 'nullable': True,
                            'metadata': {}},
                           {'name': 'season', 'type': 'string', 'nullable': True, 'metadata': {}}]}


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

    table_creation_protocol = repo.CreateManagedDeltaTableSQL

    temp_table_name = "_temp_my_hive_table_2"

    partition_columns = ("name",)

    pruning_column = 'name'

    schema = T.StructType(
        [
            T.StructField('id', T.StringType(), True),
            T.StructField('isDeleted', T.StringType(), True),
            T.StructField('name', T.StringType(), True),
            T.StructField('pythons',
                          T.ArrayType(T.StructType([T.StructField('id', T.StringType(), True)]), True),
                          True),
            T.StructField('season', T.StringType(), True),
            T.StructField('onStream', T.StringType(), False)
        ])

    def after_initialise(self):
        self.perform_table_creation_protocol()

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"


class MyHiveTable2RequiringMergeSchema(repo.HiveRepo):
    table_name = "my_hive_table_2"

    table_creation_protocol = repo.CreateManagedDeltaTableSQL

    temp_table_name = "_temp_my_hive_table_2"

    partition_columns = ("name",)

    pruning_column = 'name'

    def schema_(self):
        return T.StructType(
            [
                T.StructField('id', T.StringType(), True),
                T.StructField('name', T.StringType(), True),
                T.StructField('pythons',
                              T.ArrayType(T.StructType([T.StructField('id', T.StringType(), True)]), True),
                              True),
                T.StructField('season', T.StringType(), True),
                T.StructField('onStream', T.StringType(), False)
            ])

    def after_initialise(self):
        self.perform_table_creation_protocol()

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"


class MyHiveTable3(repo.HiveRepo):
    table_name = "my_hive_table_2"

    table_creation_protocol = repo.CreateUnManagedDeltaTableSQL

    partition_columns = ("name",)

    pruning_column = 'name'

    schema = T.StructType(
        [
            T.StructField('id', T.StringType(), True),
            T.StructField('name', T.StringType(), True),
            T.StructField('pythons',
                          T.ArrayType(T.StructType([T.StructField('id', T.StringType(), False)]), False),
                          False),
            T.StructField('season', T.StringType(), True),
            T.StructField('onStream', T.StringType(), False)
        ])

    def after_initialise(self):
        self.perform_table_creation_protocol()

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"


class MyBadlyConfiguredHiveTable(repo.HiveRepo):
    pass


class MyHiveTable4(repo.HiveRepo):
    table_name = "my_hive_table_4"

    partition_columns = ("name",)

    pruning_column = 'name'

    schema = T.StructType(
        [
            T.StructField('id', T.StringType(), True),
            T.StructField('name', T.StringType(), True),
            T.StructField('pythons',
                          T.ArrayType(T.StructType([T.StructField('id', T.StringType(), True)]), True),
                          False),
            T.StructField('season', T.StringType(), True),
            T.StructField('onStream', T.StringType(), False)
        ])


class TableWithSchemaAsDict(repo.HiveRepo):
    table_name = "t1"

    def schema_(self):
        return {'fields': [
            {'metadata': {}, 'name': 'id', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'name', 'nullable': True, 'type': 'string'}
        ], 'type': 'struct'}


class TableWithSchemaInClsAttr(repo.HiveRepo):
    table_name = "t1"

    schema = T.StructType(
        [
            T.StructField('id', T.StringType(), True),
            T.StructField('name', T.StringType(), True)
        ])


class TableWithSchemaInClsAttrAsDict(repo.HiveRepo):
    table_name = "t1"

    schema = {'fields': [
        {'metadata': {}, 'name': 'id', 'nullable': True, 'type': 'string'},
        {'metadata': {}, 'name': 'name', 'nullable': True, 'type': 'string'}
    ], 'type': 'struct'}


class TableWithSchemaInMethod(repo.HiveRepo):
    table_name = "t1"

    def schema_(self):
        return T.StructType(
            [
                T.StructField('id', T.StringType(), True),
                T.StructField('name', T.StringType(), True)
            ])


class TableWithSchemaInMethodAsDict(repo.HiveRepo):
    table_name = "t1"

    def schema_(self):
        return {'fields': [
            {'metadata': {}, 'name': 'id', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'name', 'nullable': True, 'type': 'string'}
        ], 'type': 'struct'}


def my_table_df(db):
    return db.session.read.json("tests/fixtures/table1_rows.json", multiLine=True, prefersDecimal=True)


def my_table_2_df(db):
    return db.session.read.json("tests/fixtures/table2_rows.json", multiLine=True, prefersDecimal=True)


def my_table_df_new_rows(db):
    return db.session.read.json("tests/fixtures/table1_rows_2.json", multiLine=True, prefersDecimal=True)


def my_table_df_deleted_rows(db):
    return db.session.read.json("tests/fixtures/table1_rows_with_delete.json", multiLine=True, prefersDecimal=True)


def my_table_df_updated_row(db):
    return db.session.read.json("tests/fixtures/table1_rows_3.json", multiLine=True, prefersDecimal=True)
