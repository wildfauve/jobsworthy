import pytest

from jobsworthy import spark_job
from jobsworthy.util import error


def it_creates_a_general_config():
    cfg = spark_job.Config(data_product_name="my_data_product_name",
                           domain_name="my_domain",
                           service_name="my_service")

    cfg.configure_hive_db(db_name="my_db")

    assert cfg.domain_name == "my_domain"
    assert cfg.db.db_name == "my_db"


def it_create_a_job_config():
    cfg = spark_job.JobConfig(data_product_name="my_data_product_name",
                              domain_name="my_domain",
                              service_name="my_service")

    cfg.configure_hive_db(db_name="my_db")

    assert cfg.domain_name == "my_domain"
    assert cfg.db.db_name == "my_db"


def it_normalises_names_to_snake_case():
    cfg = spark_job.JobConfig(data_product_name="myDataProductName",
                              domain_name="myDomain",
                              service_name="myService").configure_hive_db(db_name="myDb")

    assert cfg.db.db_name == "my_db"
    assert cfg.data_product_name == "my_data_product_name"
    assert cfg.domain_name == "my_domain"


def it_fails_when_cosmos_is_not_configured_correctly():
    with pytest.raises(error.RepoConfigError):
        (spark_job.JobConfig(data_product_name="myDataProductName",
                               domain_name="myDomain",
                               service_name="myService")
           .configure_cosmos_db(db_name="cosmos_db_name",
                                container_name="cosmos_container_name"))
