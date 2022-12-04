import pytest
import json
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


def test_create_hive_table_in_db(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    assert not my_table.table_exists()

    my_table.create_as_unmanaged_delta_table()

    assert my_table.table_exists()


def test_on_init_callback_create_table(test_db):
    my_table = tables.MyHiveTableWithCallbacks(db=test_db)

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

    expected_col_spec = '( id string, name string, pythons array<struct<id:string>>, season string, onStream string )'

    assert col_spec == expected_col_spec


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

    df_schema = json.loads(df.schema.json())

    assert my_table.schema_as_dict() == df_schema