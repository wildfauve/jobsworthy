from jobsworthy.util import error

from tests.shared import *
from tests.shared import config_for_testing


def test_reads_secret_from_scope(test_container):
    provider = secrets.Secrets(session=test_container.session,
                               config=job_config(),
                               secrets_provider=dbutils_wrapper()).clear_cache()

    the_secret = provider.get_secret("my_secret")

    assert the_secret.is_right()

    assert the_secret.value == "from: my_domain.my_data_product_name/my_secret generates a-secret"


def test_reads_secret_from_alternate_scope(test_container):
    test_secrets = {"my_domain.my_data_product_name": {'my_secret': 'a-secret'},
                    "alt_scope": {'my_secret': 'b-secret'}}

    provider = secrets.Secrets(
        session=test_container.session,
        config=job_config(),
        secrets_provider=dbutils_wrapper(test_secrets)).clear_cache()

    main_secret = provider.get_secret(secret_name="my_secret")
    alt_secret = provider.get_secret(non_default_scope_name="alt_scope", secret_name="my_secret")

    assert main_secret.is_right() and alt_secret.is_right()
    assert main_secret.value == "a-secret"
    assert alt_secret.value == "b-secret"


def test_cache_for_non_default_scopes(test_container):
    test_secrets = {"my_domain.my_data_product_name": {'my_secret': 'a-secret'},
                    "alt_scope": {'my_secret': 'b-secret'}}

    provider = secrets.Secrets(
        session=test_container.session,
        config=job_config(),
        secrets_provider=dbutils_wrapper(test_secrets)).clear_cache()

    provider.get_secret(secret_name="my_secret")
    provider.get_secret(non_default_scope_name="alt_scope", secret_name="my_secret")

    provider.secrets_cache == {'my_domain.my_data_product_name': {'my_secret': 'a-secret'},
                               'alt_scope': {'my_secret': 'b-secret'}}


def test_caches_secret(test_container):
    provider = secrets.Secrets(session=test_container.session,
                               config=job_config(),
                               secrets_provider=dbutils_wrapper()).clear_cache()

    provider.get_secret("my_secret")

    assert provider.secrets_cache == {
        'my_domain.my_data_product_name': {
            'my_secret': 'from: my_domain.my_data_product_name/my_secret generates a-secret'}
    }


def test_with_loaded_secrets(test_container):
    test_secrets = {"my_domain.my_data_product_name":
                        {'my_secret': 'a-secret'}
                    }

    provider = secrets.Secrets(
        session=test_container.session,
        config=job_config(),
        secrets_provider=dbutils_wrapper(test_secrets)).clear_cache()

    the_secret = provider.get_secret("my_secret")

    assert the_secret.value == "a-secret"


def test_secret_not_available(test_container):
    test_secrets = {"my_domain.my_data_product_name":
                        {'my_secret': 'a-secret'}
                    }

    provider = secrets.Secrets(session=test_container.session,
                               config=job_config(),
                               secrets_provider=dbutils_wrapper(test_secrets)).clear_cache()

    the_secret = provider.get_secret("not_a_secret_key")

    assert the_secret.is_left()
    assert isinstance(the_secret.error(), error.SecretError)
    assert the_secret.error().message == "Secret does not exist with scope: my_domain.my_data_product_name and key: not_a_secret_key"


def test_override_scope(test_container):
    test_secrets = {
        "overridden_scope":
            {'my_secret': 'a-secret'}
    }

    provider = secrets.Secrets(session=test_container.session,
                               config=job_config(),
                               secrets_provider=dbutils_wrapper(test_secrets),
                               default_scope_name="overridden_scope").clear_cache()

    the_secret = provider.get_secret("my_secret")

    assert the_secret.value == "a-secret"


def test_invalid_provider(test_container):
    result = secrets.Secrets(session=test_container.session,
                             config=job_config(),
                             secrets_provider=databricks.DatabricksUtilMockWrapper).get_secret(
        "blah")

    assert result.is_left()


def test_session_initialised(test_container):
    from pyspark.sql import session
    from tests.shared import dependencies as deps

    the_secret = deps.secrets_provider().get_secret("CosmosDBAuthorizationKey",
                                                    non_default_scope_name="my_secrets_scope")

    assert the_secret.value == "a-secret"
    assert isinstance(deps.secrets_provider().utils().session, session.SparkSession)


def test_client_credentials(test_container):
    test_secrets = {
        "my_domain.my_data_product_name":
            {'client_id': 'my_client_id', 'client_secret': "my_client_secret", "tenant_id": "my_tenant_id"}
    }

    class MockClientCredentialsProvider:
        def __init__(self, *args):
            self.args = args

    provider = secrets.Secrets(session=test_container.session,
                               config=job_config(),
                               secrets_provider=dbutils_wrapper(test_secrets),
                               client_credentials_provider=MockClientCredentialsProvider).clear_cache()

    credential = provider.client_credential_grant()

    assert credential.is_right()

    assert credential.value.args == ("my_tenant_id", "my_client_id", "my_client_secret")


#
# Helpers
#
def job_config():
    return (spark_job.JobConfig(data_product_name="my_data_product_name",
                                domain_name="my_domain",
                                service_name="my_service",
                                client_id_key='client_id',
                                client_secret_key='client_secret',
                                tenant_key='tenant_id')
            .configure_hive_db(db_name="my_db",
                               db_file_system_path_root=config_for_testing.DB_FILE_SYSTEM_PATH_ROOT,
                               db_path_override_for_checkpoint=config_for_testing.CHECKPOINT_OVERRIDE))
