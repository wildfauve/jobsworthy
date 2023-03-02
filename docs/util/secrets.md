# Secrets

The Secrets module obtains secrets using the Databricks DBUtils secrets utility. The module acts as a wrapper for
DButils. This allows for secrets to be mocked in tests without needing DBUtils. The CosmosDB repository is injected with
the secrets provider to enable secured access to CosmosDB.

The provider requires access to the Spark session when running on Databricks. However this is not required in test. You
also provide Secrets with a wrapper for DBUtils with also, optionally, takes a session. Both test and production
wrappers are available in the `util.databricks` module.

```python
from jobsworthy.util import secrets, databricks

provider = secrets.Secrets(session=di_container.session,
                           config=job_config(),
                           secrets_provider=databricks.DatabricksUtilsWrapper())
```

The default secret scope name is defined from the `JobConfig` properties; `domain_name` and `data_product_name`,
separated by a `.`. This can be overridden by defining the scope on the `Secrets` constructor, or on the call
to `get_secret`. It looks like this on the constructor.

```python
provider = secrets.Secrets(session=di_container.session,
                           config=job_config(),
                           secrets_provider=databricks.DatabricksUtilsWrapper(),
                           default_scope_name="custom-scope-name")
```

Getting a secret.

```python
provider.get_secret(secret_name="name-of-secret")  # returns an Either[secret-key]
```

Secrets is also able to return a `ClientCredential` using an Azure AD client credentials grant. The grant requires that
client id and secrets are obtainable via DBUtils through key-vault with the key names defined in `JobConfig` in the
properties `client_id_key` and `client_secret_key`

```python
provider.client_credential_grant()  # returns an Either[ClientCredential]
```

Testing using secrets. DBUtils is not available as an open source project. When creating the secrets provider, you can
provide a DBUtils mock class which is available. On this class you can also construct valid keys to be used for test (if
required; the mock returns a dummy key response to any generic lookup).

The example below also shows how to use a non-default scope on the `get_secrets` function.

```python
from jobsworthy.util import secrets, databricks

test_secrets = {"my_domain.my_data_product_name": {'my_secret': 'a-secret'},
                "alt_scope": {'my_secret': 'b-secret'}}

provider = secrets.Secrets(
    session=di_container.session,
    config=job_config(),
    secrets_provider=databricks.DatabricksUtilMockWrapper(spark_test_session.MockPySparkSession, test_secrets))

provider.get_secret(non_default_scope_name="alt_scope", secret_name="my_secret")
```
