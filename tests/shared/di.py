import pytest
from dependency_injector import containers, providers
from jobsworthy.repo import cosmos_repo, hive_repo, spark_db
from jobsworthy.util import databricks, secrets
from jobsworthy.util import session as spark_session

from tests.shared import config_for_testing, cosmos_fixture, spark_test_session


class LocalContainer(containers.DeclarativeContainer):
    session = providers.Callable(
        spark_session.build_spark_session,
        "test_spark_session",
        spark_test_session.spark_delta_session,
        spark_test_session.spark_session_config,
    )

    job_config = providers.Callable(config_for_testing.build_job_config)

    db_utils = providers.Factory(databricks.DatabricksUtilMockWrapper,
        None,
        load_secrets={
            f"{config_for_testing.SECRETS_SCOPE}": {"CosmosDBAuthorizationKey": "a-secret"}
        })

    secrets_provider = providers.Factory(
        secrets.Secrets,
        session,
        job_config,
        db_utils,
        config_for_testing.SECRETS_SCOPE
    )

    database = providers.Factory(spark_db.Db, session, job_config)


@pytest.fixture
def local_container():
    cosmos_fixture.MockCosmosDBStreamReader.db_table_name = (
        f"{config_for_testing.DATABASE_NAME}.cosmosmock"
    )
    di = init_container()
    return di


def init_container():
    di = LocalContainer()
    # di.config.from_dict(config_for_testing.config)
    di.wire(modules=['tests.shared.dependencies'])
    return di
