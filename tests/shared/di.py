import pytest
from dependency_injector import containers, providers
from jobsworth.repo import cosmos_repo, hive_repo, spark_db
from jobsworth.util import databricks, secrets, session, env

from tests.shared import config_for_testing, cosmos_fixture, spark_test_session


class TestContainer(containers.DeclarativeContainer):

    session = providers.Callable(
        session.build_spark_session,
        "test_spark_session",
        spark_test_session.spark_delta_session,
        spark_test_session.spark_session_config,
    )

    job_config = providers.Callable(config_for_testing.build_job_config)

    secrets_provider = providers.Factory(
        secrets.Secrets,
        job_config,
        databricks.DatabricksUtilMockWrapper(
            session,
            load_secrets={
                f"{config_for_testing.SECRETS_SCOPE}": {"CosmosDBAuthorizationKey": "a-secret"}
            }
        ),
        config_for_testing.SECRETS_SCOPE
    )

    database = providers.Factory(spark_db.Db, session, job_config)

@pytest.fixture
def test_container():
    cosmos_fixture.MockCosmosDBStreamReader.db_table_name = (
        f"{config_for_testing.DATABASE_NAME}.cosmosmock"
    )
    di = init_container()
    return di


def init_container():
    di = TestContainer()
    # di.config.from_dict(config_for_testing.config)
    di.wire(modules=['tests.shared.dependencies'])
    return di
