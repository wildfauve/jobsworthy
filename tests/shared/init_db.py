import pytest

from jobsworthy import spark_job
from jobsworthy import repo

from . import spark_test_session, config_for_testing

class MyDb(repo.Db):
    db_properties = [
        repo.DbProperty(repo.DataAgreementType.DATA_PRODUCT, "my_data_product", "my_namespace"),
        repo.DbProperty(repo.DataAgreementType.DESCRIPTION,
                        "DB for Data Product 1",
                        "my_namespace"),
    ]

class MyDbWithoutProps(repo.Db):
    pass

@pytest.fixture
def test_db():
    db = MyDb(session=spark_test_session.create_session(), job_config=job_cfg())

    yield db

    db.drop_db()


@pytest.fixture
def test_db_cosmos_connection_string():
    db = MyDb(session=spark_test_session.create_session(), job_config=job_cfg_with_cosmos_connection_string())

    yield db

    db.drop_db()

@pytest.fixture
def test_db_without_props():
    db = MyDbWithoutProps(session=spark_test_session.create_session(), job_config=job_cfg())

    yield db

    db.drop_db()


@pytest.fixture
def test_db_domain_naming_convention():
    db = repo.Db(session=spark_test_session.create_session(),
                 job_config=job_cfg(),
                 naming_convention=repo.DbNamingConventionDomainBased)

    yield db

    db.drop_db()


@pytest.fixture
def job_cfg_fixture():
    return job_cfg()


def job_cfg():
    return (spark_job.JobConfig(data_product_name="my_data_product_name",
                                domain_name="my_domain",
                                service_name="my_service")
            .configure_hive_db(db_name="my_db",
                               db_file_system_path_root=config_for_testing.DB_FILE_SYSTEM_PATH_ROOT,
                               db_path_override_for_checkpoint=config_for_testing.CHECKPOINT_OVERRIDE)
            .configure_cosmos_db(account_key_name="my_cosmos_account_key",
                                 endpoint="cosmos_endpoint",
                                 db_name="cosmos_db_name",
                                 container_name="cosmos_container_name")
            .running_in_test())


def job_cfg_with_cosmos_connection_string():
    return (spark_job.JobConfig(data_product_name="my_data_product_name",
                                domain_name="my_domain",
                                service_name="my_service")
            .configure_hive_db(db_name="my_db",
                               db_file_system_path_root=config_for_testing.DB_FILE_SYSTEM_PATH_ROOT,
                               db_path_override_for_checkpoint=config_for_testing.CHECKPOINT_OVERRIDE)
            .configure_cosmos_db(connection_string_key_name="my_cosmos_connection_string",
                                 db_name="cosmos_db_name",
                                 container_name="cosmos_container_name")
            .running_in_test())
