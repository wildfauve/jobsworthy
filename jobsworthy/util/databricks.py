from typing import Dict
from contextlib import contextmanager

from . import fn


class DatabricksUtilsWrapper:
    """
    DBUtils is the databricks utils library available on any databricks cluster.  It is obtained via pyspark.  This
    utility is not available outside the cluster, therefore this module provides a wrapper for obtaining dbutils in a
    way that supports local testing.  In a test, use DatabricksUtilMockWrapper.
    """

    def __init__(self, session=None) -> None:
        self.session = session
        self.db_utils = None
        pass

    def utils(self, spark_session=None):
        if self.db_utils:
            return self.db_utils

        from pyspark.dbutils import DBUtils
        self.db_utils = DBUtils(self.build_spark_session(spark_session))
        return self.db_utils

    def build_spark_session(self, provided_session=None):
        return provided_session if provided_session else self.session()



class DatabricksUtilMockWrapper:
    """
    Provides a test mock for DBUtils when running locally.  The mock supports the following DBUtils commands:
    + dbutils.secrets
    """

    def __init__(self, session=None, load_secrets: Dict = None):
        self.session = session
        self.load_secrets = load_secrets
        self.db_utils = None

    def utils(self, spark_session=None):
        if self.db_utils:
            return self.db_utils

        self.db_utils = MockDBUtils(self.build_spark_session(spark_session), self.load_secrets)
        return self.db_utils

    def build_spark_session(self, provided_session=None):
        return provided_session if provided_session else self.session


class MockDBUtils:
    def __init__(self, session, secrets_to_load: Dict):
        self.session = session
        self.secrets = SecretMock(load_secrets=secrets_to_load)


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
