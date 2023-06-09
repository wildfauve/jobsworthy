from pyspark.sql import session
from dependency_injector.providers import Callable
from tests.shared import *


def test_container_session(local_container):
    from tests.shared import dependencies as deps

    assert isinstance(deps.db().session, session.SparkSession)
    assert isinstance(deps.secrets_provider().utils().session, session.SparkSession)
