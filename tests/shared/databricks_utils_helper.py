from typing import Dict
from jobsworthy.util import databricks
from . import spark_test_session


def dbutils_wrapper(required_secret_config: Dict = None):
    return databricks.DatabricksUtilMockWrapper(spark_test_session.MockPySparkSession, required_secret_config)

