import pytest

from jobsworthy import spark_job
from jobsworthy import repo
from jobsworthy.util import error

from tests.shared import spark_test_session, tables, config_for_testing


def test_delta_upsert_no_change(test_db):
    my_table = tables.MyHiveTable(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    df = my_table.read()

    assert df.count() == 2

    my_table.upsert(df)

    df = my_table.read()
    assert df.count() == 2

    sketches = [(row.id, row.name) for row in df.select(df.id, df.name).collect()]

    expected_results = [('https://example.nz/montyPython/sketches/theSpanishInquisition', 'The Spanish Inquisition'),
                        ('https://example.nz/montyPython/sketches/thePiranhaBrothers', 'The Piranha Brothers')]

    assert sketches == expected_results


def test_partitioned_delta_upsert_with_new_rows(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    my_table.try_upsert(tables.my_table_df(test_db))

    assert my_table.read().count() == 2

    my_table.upsert(tables.my_table_df_new_rows(test_db))

    df = my_table.read()

    assert df.count() == 4

    sketches = set([(row.id, row.name) for row in df.select(df.id, df.name).collect()])

    expected_rows = set([('https://example.nz/montyPython/sketches/thePiranhaBrothers', 'The Piranha Brothers'),
                         ('https://example.nz/montyPython/sketches/theSpanishInquisition', 'The Spanish Inquisition'),
                         ('https://example.nz/montyPython/sketches/ericTheHalfBee', 'Eric the Half Bee'),
                         ('https://example.nz/montyPython/sketches/theCheeseShop', 'The Cheese Shop')])

    assert sketches == expected_rows


def test_upsert_updates_row(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    my_table.try_upsert(tables.my_table_df(test_db))

    assert my_table.read().count() == 2

    my_table.upsert(tables.my_table_df_updated_row(test_db))

    df = my_table.read()

    assert df.count() == 2

    new_pythons = [python.id for row in
                   df.filter(df.id == 'https://example.nz/montyPython/sketches/thePiranhaBrothers').select(
                       df.pythons).collect() for
                   python in row.pythons]

    expected_updates = ['https://example.nz/montyPython/michealPalin',
                        'https://example.nz/montyPython/johnCleese',
                        'https://example.nz/montyPython/ericIdol']

    assert new_pythons == expected_updates


def test_try_upsert(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    my_table.try_upsert(tables.my_table_df(test_db))

    result = my_table.try_upsert(tables.my_table_df_new_rows(test_db))

    assert result.is_right()
    assert my_table.read().count() == 4


def test_non_partitioned_delta_upsert_with_new_rows(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    my_table.try_upsert(tables.my_table_df(test_db))

    assert my_table.read().count() == 2

    my_table.upsert(tables.my_table_df_new_rows(test_db))

    assert my_table.read().count() == 4


def test_errors_when_delta_upserting_in_test_without_checkpoint_override():
    db = repo.Db(session=spark_test_session.create_session(), job_config=job_config_without_checkpoint_override())

    my_table = tables.MyHiveTable(db=db)

    my_table.try_upsert(tables.my_table_df(db))

    with pytest.raises(error.RepoConfigError):
        my_table.upsert(tables.my_table_df_new_rows(db))

    db.drop_db()


def test_delta_table_read_and_spark_load_of_delta(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    my_table.upsert(tables.my_table_df(test_db))

    my_table.upsert(tables.my_table_df_new_rows(test_db))

    df_from_delta_read = my_table.delta_read()
    df_from_spark_read = my_table.read()

    assert df_from_delta_read.count() == 4
    assert df_from_spark_read.count() == 4


def test_delta_upsert_fails_when_no_identity_condition_provided(test_db):
    my_table = tables.MyHiveTableWithoutIdentityCondition(db=test_db)

    my_table.upsert(tables.my_table_df(test_db))

    with pytest.raises(error.RepoConfigError):
        my_table.upsert(tables.my_table_df_new_rows(test_db))


def test_partitions_using_repo_declared_partitions_on_upsert(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    my_table.upsert(tables.my_table_df(test_db))

    df = my_table.read()

    assert df.rdd.getNumPartitions() == 2


def test_partitions_using_repo_declared_partitions_on_append(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    my_table.write_append(tables.my_table_df(test_db))

    df = my_table.read()

    assert df.rdd.getNumPartitions() == 2


def test_delete_condition(test_db):
    my_table = tables.MyHiveTableWithUpdataAndDeleteCondition(db=test_db)

    my_table.try_upsert(tables.my_table_df(test_db))

    assert my_table.read().count() == 2

    my_table.upsert(tables.my_table_df_deleted_rows(test_db))

    df = my_table.read()

    assert df.count() == 1


#
# Helpers
#
def job_config():
    return (spark_job.JobConfig(data_product_name="my_data_product_name",
                                domain_name="my_domain",
                                service_name="my_service")
            .configure_hive_db(db_name="my_db",
                               db_file_system_path_root=config_for_testing.DB_FILE_SYSTEM_PATH_ROOT,
                               db_path_override_for_checkpoint=config_for_testing.CHECKPOINT_OVERRIDE))


def job_config_without_checkpoint_override():
    return (spark_job.JobConfig(data_product_name="my_data_product_name",
                                domain_name="my_domain",
                                service_name="my_service")
            .configure_hive_db(db_name="my_db",
                               db_file_system_path_root=config_for_testing.DB_FILE_SYSTEM_PATH_ROOT)
            .running_in_test())
