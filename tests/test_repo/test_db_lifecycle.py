import pytest
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



def test_create_hive_table_in_db(test_db):
    my_table = tables.MyHiveTable(db=test_db)

    assert not my_table.table_exists()

    my_table.write_append(tables.my_table_df(test_db))

    assert my_table.table_exists()


def test_table_doesnt_provide_table_name(test_db):
    with pytest.raises(error.RepoConfigError):
        tables.MyBadlyConfiguredHiveTable(db=test_db)


