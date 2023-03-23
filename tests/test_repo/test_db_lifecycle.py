import pytest
import json
from pyspark.sql import types as T
from jobsworthy import repo
from jobsworthy.util import error

from tests.shared import spark_test_session, tables, config_for_testing


def test_initialise_db():
    db = repo.Db(session=spark_test_session.create_session(), job_config=config_for_testing.build_job_config())

    assert db.db_exists()

    db.drop_db()

    assert not db.db_exists()


def test_drop_table(test_db):
    my_table = tables.MyHiveTable(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    assert my_table.table_exists()

    my_table.drop_table()

    assert not my_table.table_exists()


def test_create_hive_table_in_db_from_dataframe(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    assert not my_table.table_exists()

    my_table.write_append(tables.my_table_df(test_db))

    assert my_table.table_exists()


def test_table_doesnt_provide_table_name(test_db):
    with pytest.raises(error.RepoConfigError):
        tables.MyBadlyConfiguredHiveTable(db=test_db)


def test_create_unmanaged_hive_table_in_db(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    assert not my_table.table_exists()

    my_table.create_as_unmanaged_delta_table()

    assert my_table.table_exists()


def test_create_managed_hive_table_in_db(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    assert not my_table.table_exists()

    my_table.create_as_managed_delta_table()

    assert my_table.table_exists()


def test_create_managed_table_using_protocol(test_db):
    my_table = tables.MyHiveTable(db=test_db, table_creation_protocol=repo.CreateManagedDeltaTableSQL)

    assert not my_table.table_exists()

    my_table.perform_table_creation_protocol()

    assert my_table.table_exists()


def test_create_unmanaged_table_using_protocol(test_db):
    my_table = tables.MyHiveTable(db=test_db, table_creation_protocol=repo.CreateUnManagedDeltaTableSQL)

    assert not my_table.table_exists()

    my_table.perform_table_creation_protocol()

    assert my_table.table_exists()


# def test_create_managed_hive_table_using_dataframe_protocol(test_db):
#     my_table = tables.MyHiveTable4(db=test_db,
#                                    table_creation_protocol=repo.CreateManagedDeltaTableFromEmptyDataframe)
#
#     assert not my_table.table_exists()
#
#     my_table.perform_table_creation_protocol()
#     breakpoint()
#
#     assert my_table.table_exists()
#


def test_on_init_callback_create_table(test_db):
    my_table = tables.MyHiveTableCreatedAsManagedTable(db=test_db)

    assert my_table.table_exists()

    props = my_table.property_manager.get_table_properties()

    expected_keys = ['delta.minReaderVersion', 'delta.minWriterVersion', 'urn:my_namespace:catalogue:description',
                     'urn:my_namespace:dataProduct:port', 'urn:my_namespace:dq:updateFrequency',
                     'urn:my_namespace:spark:table:schema:partitionColumns',
                     'urn:my_namespace:spark:table:schema:pruneColumn', 'urn:my_namespace:spark:table:schema:version']

    assert [p.key for p in props.select(props.key).collect()] == expected_keys


def test_table_has_a_schema(test_db):
    my_table = tables.MyHiveTable3(db=test_db)

    assert my_table.has_specified_schema()


def test_generates_column_specification_from_struct_based_schema(test_db):
    my_table = tables.MyHiveTable3(db=test_db)

    col_spec = my_table.column_specification_from_schema()

    expected_col_spec = \
        '( id string, name string, pythons array<struct<id:string>> NOT NULL, season string, onStream string NOT NULL )'

    assert col_spec == expected_col_spec


def test_schema_locations(test_db):
    table_with_schema_in_cls_attr = tables.TableWithSchemaInClsAttr(db=test_db)
    table_with_schema_in_cls_attr_as_dict = tables.TableWithSchemaInClsAttrAsDict(db=test_db)
    table_with_schema_in_method = tables.TableWithSchemaInMethod(db=test_db)
    table_with_schema_in_method_as_dict = tables.TableWithSchemaInMethodAsDict(db=test_db)
    table_with_schema_in_dict_method = tables.TableWithSchemaAsDict(db=test_db)

    assert table_with_schema_in_cls_attr.has_specified_schema()
    assert table_with_schema_in_cls_attr_as_dict
    assert table_with_schema_in_method.has_specified_schema()
    assert table_with_schema_in_dict_method.has_specified_schema()
    assert table_with_schema_in_method_as_dict.has_specified_schema()

    target_struct_schema = T.StructType(
        [T.StructField('id', T.StringType(), True), T.StructField('name', T.StringType(), True)])

    assert table_with_schema_in_cls_attr.schema_as_struct() == target_struct_schema
    assert table_with_schema_in_cls_attr_as_dict.schema_as_struct() == target_struct_schema
    assert table_with_schema_in_method.schema_as_struct() == target_struct_schema
    assert table_with_schema_in_dict_method.schema_as_struct() == target_struct_schema
    assert table_with_schema_in_method_as_dict.schema_as_struct() == target_struct_schema



def test_generates_column_specification_from_json_based_schema(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    col_spec = my_table.column_specification_from_schema()

    expected_col_spec = '( id string, name string, pythons array<struct<id:string>>, season string )'

    assert col_spec == expected_col_spec


def test_json_based_schema_on_create_df(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    with open("tests/fixtures/table1_rows.json") as f:
        data = json.load(f)

    df = my_table.create_df(data)

    df_schema = df.schema.jsonValue()

    assert my_table.schema_() == df_schema
