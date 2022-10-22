import pytest
from pyspark.sql import functions as F

from jobsworth import config
from jobsworth.repo import spark_db, hive_repo
from jobsworth.util import error

from tests.shared import spark_test_session, table_setup


def test_initialise_db():
    db = spark_db.Db(session=spark_test_session.create_session(), config=job_config())

    assert db.db_exists()

    db.drop_db()

    assert not db.db_exists()


def test_create_hive_table_in_db(test_db):
    my_table = MyHiveTable(db=test_db)

    assert not my_table.table_exists()

    my_table.create(my_table_df(test_db))

    assert my_table.table_exists()

def test_add_table_properties_on_create(test_db):
    my_table = MyHiveTable(db=test_db)
    my_table.create(my_table_df(test_db))


    props = my_table.urn_table_properties()

    assert len(props) == 1
    assert props[0].key == "urn:my_namespace:spark:table:schema:version"
    assert props[0].value == "0.0.1"

def test_doesnt_add_table_props_when_none_defined(test_db):
    my_table = MyHiveTable2(db=test_db)
    my_table.create(my_table_df(test_db))

    props = my_table.urn_table_properties()

    assert not props



def test_read_table(test_db):
    my_table = MyHiveTable(db=test_db)
    my_table.create(my_table_df(test_db))

    df = my_table.read()
    assert df.columns == ['id', 'name', 'pythons', 'season']

    sketches = [row.name for row in df.select(df.name).collect()]

    assert sketches == ['The Piranha Brothers', 'The Spanish Inquisition']


def test_delta_upsert_no_change(test_db):
    my_table = MyHiveTable(db=test_db)
    my_table.create(my_table_df(test_db), "name")

    df = my_table.read()

    assert df.count() == 2

    my_table.upsert(df, "name", "name")

    assert my_table.read().count() == 2



def test_partitioned_delta_upsert_with_new_rows(test_db):
    my_table = MyHiveTable(db=test_db)
    my_table.create(my_table_df(test_db), "name")

    assert my_table.read().count() == 2

    my_table.upsert(my_table_df_new_rows(test_db), "name", "name")

    assert my_table.read().count() == 4


def test_try_upsert(test_db):
    my_table = MyHiveTable(db=test_db)
    my_table.create(my_table_df(test_db), "name")

    result = my_table.try_upsert(my_table_df_new_rows(test_db), "name", "name")

    assert result.is_right()
    assert my_table.read().count() == 4

def test_non_partitioned_delta_upsert_with_new_rows(test_db):
    my_table = MyHiveTable(db=test_db)
    my_table.create(my_table_df(test_db))

    assert my_table.read().count() == 2

    my_table.upsert(my_table_df_new_rows(test_db))

    assert my_table.read().count() == 4


def test_delta_upsert_fails_when_no_identity_condition_provided(test_db):
    my_table = MyHiveTableWithoutIdentityCondition(db=test_db)
    my_table.create(my_table_df(test_db))

    assert my_table.read().count() == 2

    with pytest.raises(error.RepoConfigError):
        my_table.upsert(my_table_df_new_rows(test_db), "id")



def test_read_write_streams_with_partitions(test_db):
    my_table = MyHiveTable(db=test_db)
    my_table.create(my_table_df(test_db))

    my_table_2 = MyHiveTable2(db=test_db,
                              reader=hive_repo.DeltaFileReader,
                              stream_writer=hive_repo.StreamFileWriter)

    stream = my_table.read_stream()

    df = stream.withColumn('onStream', F.lit("true"))

    my_table_2.write_stream(df, ("name",))

    my_table_2.await_termination()

    table_2_df = my_table_2.read()

    assert "onStream" in table_2_df.columns
    assert table_2_df.rdd.getNumPartitions() == 2


def test_cant_use_hive_stream_writer_in_test(test_db):
    my_table = MyHiveTable(db=test_db)
    my_table.create(my_table_df(test_db))

    my_table_2 = MyHiveTable2(db=test_db,
                              reader=hive_repo.DeltaFileReader,
                              stream_writer=hive_repo.StreamHiveWriter)

    stream = my_table.read_stream()

    df = stream.withColumn('onStream', F.lit("true"))

    with pytest.raises(error.RepoConfigError):
        my_table_2.write_stream(df)


def test_table_doesnt_provide_table_name(test_db):
    with pytest.raises(error.RepoConfigError):
        MyBadlyConfiguredHiveTable(db=test_db)


#
# Helpers
#
class MyHiveTable(hive_repo.HiveRepo):
    table_name = "my_hive_table"
    table_properties = [
        hive_repo.TableProperty("my_namespace:spark:table:schema:version", "0.0.1")
    ]


    def identity_merge_condition(self, name_of_baseline, update_name):
        return f"{name_of_baseline}.id = {update_name}.id"


class MyHiveTableWithoutIdentityCondition(hive_repo.HiveRepo):
    table_name = "my_hive_table"


class MyHiveTable2(hive_repo.HiveRepo):
    table_name = "my_hive_table_2"


class MyBadlyConfiguredHiveTable(hive_repo.HiveRepo):
    pass


def my_table_df(db):
    return table_setup.test_df(db.session)


def my_table_df_new_rows(db):
    return table_setup.test_df_2(db.session)


def job_config():
    return config.JobConfig(data_product_name="my_data_product_name",
                            domain_name="my_domain",
                            service_name="my_service").configure_hive_db(db_name="my_db",
                                                                         db_file_system_path_root="spark-warehouse",
                                                                         checkpoint_root="tests/db")
