from typing import Dict

from . import fn


class DatabricksUtilsWrapper:
    """
    DBUtils is the databricks utils library available on any databricks cluster.  It is obtained via pyspark.  This
    utility is not available outside the cluster, therefore this module provides a wrapper for obtaining dbutils in a
    way that supports local testing.  In a test, using DatabricksUtilMockWrapper.
    """

    def __init__(self, session) -> None:
        self.session = session
        pass

    def utils(self):
        from pyspark.dbutils import DBUtils

        return DBUtils(self.session)


class DatabricksUtilMockWrapper:
    """
    Provides a test mock for DBUtils when running locally.  The mock supports the following DBUtils commands:
    + dbutils.secrets
    """

    def __init__(self, load_secrets: Dict = None):
        self.secrets = SecretMock(load_secrets=load_secrets)

    def utils(self):
        return self


class SecretMock:
    class IllegalArgumentException(Exception):
        pass

    def __init__(self, load_secrets: Dict = None):
        self.loaded_secrets = load_secrets

    def get(self, scope, key) -> str:
        if self.loaded_secrets:
            return self.return_secret_or_raise(scope, key)
        return f"from: {scope}/{key} generates a-secret"

    def return_secret_or_raise(self, scope, key):
        secret = fn.deep_get(self.loaded_secrets, [scope, key])
        if secret:
            return secret
        msg = f"Secret does not exist with scope: {scope} and key: {key}"
        raise self.IllegalArgumentException(msg)
