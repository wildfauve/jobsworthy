import pytest
import shutil

from jobsworth import config
from jobsworth.repo import spark_db

from . import spark_test_session


@pytest.fixture
def test_db():
    cfg = config.JobConfig(data_product_name="my_data_product_name",
                           domain_name="my_domain",
                           service_name="my_service").configure_db(db_name="my_db",
                                                                   db_file_system_path_root="spark-warehouse",
                                                                   checkpoint_root="tests/db")

    db = spark_db.Db(session=spark_test_session.create_session(), config=cfg)

    yield db

    db.drop_db()

    shutil.rmtree(db.table_location("my_hive_table_2"), ignore_errors=True, onerror=None)
