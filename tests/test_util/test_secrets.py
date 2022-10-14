import pytest

from jobsworth import config
from jobsworth.util import secrets, databricks, error


def test_reads_secret_from_scope():
    provider = secrets.Secrets(config=job_config(),
                               secrets_provider=databricks.DatabricksUtilMockWrapper()).clear_cache()

    the_secret = provider.get_secret("my_secret")

    assert the_secret.is_right()

    assert the_secret.value == "from: my_domain.my_service.test/my_secret generates a-secret"


def test_caches_secret():
    provider = secrets.Secrets(config=job_config(),
                               secrets_provider=databricks.DatabricksUtilMockWrapper()).clear_cache()

    provider.get_secret("my_secret")

    assert provider.secrets_cache == {'my_secret': 'from: my_domain.my_service.test/my_secret generates a-secret'}


def test_with_loaded_secrets():
    test_secrets = {"my_domain.my_service.test":
                        {'my_secret': 'a-secret'}
                    }

    provider = secrets.Secrets(config=job_config(),
                               secrets_provider=databricks.DatabricksUtilMockWrapper(
                                   load_secrets=test_secrets)).clear_cache()

    the_secret = provider.get_secret("my_secret")

    assert the_secret.value == "a-secret"


def test_secret_not_available():
    test_secrets = {"my_domain.my_service.test":
                        {'my_secret': 'a-secret'}
                    }

    provider = secrets.Secrets(config=job_config(),
                               secrets_provider=databricks.DatabricksUtilMockWrapper(
                                   load_secrets=test_secrets)).clear_cache()

    the_secret = provider.get_secret("not_a_secret_key")

    assert the_secret.is_left()
    assert isinstance(the_secret.error(), error.SecretError)
    assert the_secret.error().message == "Secret does not exist with scope: my_domain.my_service.test and key: not_a_secret_key"


def test_invalid_provider():
    with pytest.raises(error.SecretError):
        secrets.Secrets(config=job_config(),secrets_provider=databricks.DatabricksUtilMockWrapper).get_secret("blah")


#
# Helpers
#
def job_config():
    return config.JobConfig(data_product_name="my_data_product_name",
                            domain_name="my_domain",
                            service_name="my_service").configure_hive_db(db_name="my_db",
                                                                         db_file_system_path_root="spark-warehouse",
                                                                         checkpoint_root="tests/db")
