import pytest

from jobsworthy import spark_job
from jobsworthy import repo
from jobsworthy.util import error

from tests.shared import spark_test_session

def it_allows_the_checkpoint_path_to_be_overridden_in_test():
    cfg = (spark_job.JobConfig(data_product_name="my_data_product_name",
                               domain_name="my_domain",
                               service_name="my_service")
           .configure_hive_db(db_name="my_db",
                              db_file_system_path_root="domains/my_domain/data_products/my_data_product_name",
                              db_path_override_for_checkpoint="spark-warehouse/domains/my_domain/data_products/my_data_product_name"))

    db = repo.Db(session=spark_test_session.create_session(), job_config=cfg)

    assert db.naming().checkpoint_location(
        "table1") == 'spark-warehouse/domains/my_domain/data_products/my_data_product_name/my_db.db/table1/_checkpoint'
    assert db.naming().delta_table_location(
        "table1") == 'spark-warehouse/domains/my_domain/data_products/my_data_product_name/my_db.db/table1'
    assert db.naming().db_path() == 'domains/my_domain/data_products/my_data_product_name/my_db.db'


def it_uses_naming_convention_1():
    cfg = (spark_job.JobConfig(data_product_name="my_data_product_name",
                               domain_name="my_domain",
                               service_name="my_service")
           .configure_hive_db(db_name="my_db",
                              db_file_system_path_root="domains/my_domain/data_products/my_data_product_name",
                              db_path_override_for_checkpoint="spark-warehouse/domains/my_domain/data_products/my_data_product_name"))

    db = repo.Db(session=spark_test_session.create_session(), job_config=cfg)

    assert db.naming().checkpoint_location(
        "table1") == 'spark-warehouse/domains/my_domain/data_products/my_data_product_name/my_db.db/table1/_checkpoint'
    assert db.naming().delta_table_location(
        "table1") == 'spark-warehouse/domains/my_domain/data_products/my_data_product_name/my_db.db/table1'
    assert db.naming().db_path() == 'domains/my_domain/data_products/my_data_product_name/my_db.db'


def it_uses_standard_table_location_when_not_overridden():
    cfg = (spark_job.JobConfig(data_product_name="my_data_product_name",
                               domain_name="my_domain",
                               service_name="my_service")
           .configure_hive_db(db_name="my_db",
                              db_file_system_path_root="/domains/my_domain/data_products/my_data_product_name"))

    db = repo.Db(session=spark_test_session.create_session(), job_config=cfg)

    assert db.naming().checkpoint_location(
        "table1") == '/domains/my_domain/data_products/my_data_product_name/my_db.db/table1/_checkpoint'
    assert db.naming().delta_table_location(
        "table1") == '/domains/my_domain/data_products/my_data_product_name/my_db.db/table1'
    assert db.naming().db_path() == '/domains/my_domain/data_products/my_data_product_name/my_db.db'


def it_uses_relative_domain_and_data_product_naming_convention_in_test_mode():
    cfg = (spark_job.JobConfig(data_product_name="my_data_product_name",
                               domain_name="my_domain",
                               service_name="my_service")
           .configure_hive_db(db_name="my_db")
           .running_in_test())

    db = repo.Db(session=spark_test_session.create_session(),
                 job_config=cfg,
                 naming_convention=repo.DbNamingConventionDomainBased)

    assert db.naming().db_path() == "domains/my_domain/data_products/my_data_product_name/my_db.db"
    assert db.naming().delta_table_location(
        "table1") == 'spark-warehouse/domains/my_domain/data_products/my_data_product_name/my_db.db/table1'
    assert db.naming().checkpoint_location(
        "table1") == 'spark-warehouse/domains/my_domain/data_products/my_data_product_name/my_db.db/table1/_checkpoint'


def it_uses_absolute_domain_and_data_product_naming_convention_in_cluster_mode():
    cfg = (spark_job.JobConfig(data_product_name="my_data_product_name",
                               domain_name="my_domain",
                               service_name="my_service")
           .configure_hive_db(db_name="my_db"))

    db = repo.Db(session=spark_test_session.create_session(),
                 job_config=cfg,
                 naming_convention=repo.DbNamingConventionDomainBased)

    assert db.naming().db_path() == "/domains/my_domain/data_products/my_data_product_name/my_db.db"
    assert db.naming().delta_table_location(
        "table1") == '/domains/my_domain/data_products/my_data_product_name/my_db.db/table1'
    assert db.naming().checkpoint_location(
        "table1") == '/domains/my_domain/data_products/my_data_product_name/my_db.db/table1/_checkpoint'


def it_raises_config_error_when_domain_and_product_names_not_configured():
    cfg = (spark_job.JobConfig(service_name="my_service")
           .configure_hive_db(db_name="my_db"))

    with pytest.raises(error.RepoConfigError):
        repo.Db(session=spark_test_session.create_session(),
                job_config=cfg,
                naming_convention=repo.DbNamingConventionDomainBased)

