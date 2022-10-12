import pytest
import shutil

from jobsworth.repo import db_config, spark_db

from . import spark_test_session

@pytest.fixture
def test_db():
    cfg = db_config.DbConfig(db_name="my_db_name",
                             domain_name="my_domain",
                             data_product_name="my_data_product",
                             db_file_system_path_root="spark-warehouse",
                             checkpoint_root="tests/db")


    db = spark_db.Db(session=spark_test_session.create_session(), config=cfg)

    yield db

    db.drop_db()

    shutil.rmtree(db.table_location("my_hive_table_2"), ignore_errors=True, onerror=None)

