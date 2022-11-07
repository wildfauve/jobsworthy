import pytest
from pyspark.sql import functions as F

from jobsworthy.repo import spark_db, cosmos_repo
from jobsworthy.util import error, secrets, databricks

from tests.shared import spark_test_session, table_setup, cosmos_fixture, databricks_utils_helper


def test_config_options(test_db):
    my_cosmos_table = cosmos_table_setup(test_db)

    opts = my_cosmos_table.spark_config_options()

    assert opts == {'spark.cosmos.accountEndpoint': 'cosmos_endpoint',
                    'spark.cosmos.accountKey': 'from: my_domain.my_data_product_name/my_cosmos_account_key generates a-secret',
                    'spark.cosmos.database': 'cosmos_db_name',
                    'spark.cosmos.container': 'cosmos_container_name',
                    'spark.cosmos.read.inferSchema.enabled': 'true',
                    'spark.cosmos.write.strategy': 'ItemOverwrite',
                    'spark.cosmos.read.partitioning.strategy': 'Default',
                    'spark.cosmos.changeFeed.mode': 'Incremental'}


def test_reads_from_stream(test_db):
    mock_cosmos_table(test_db)
    my_cosmos_table = cosmos_table_setup(test_db)

    df = my_cosmos_table.read_stream()

    assert df.isStreaming


#
# Helpers
#
class MyCosmosTable(cosmos_repo.CosmosDb):
    pass


def mock_cosmos_table(db):
    df = table_setup.test_df(db.session)
    (df.write
     .format('delta')
     .mode("append")
     .saveAsTable('my_db.mock_cosmos_db'))


def cosmos_table_setup(db):
    secrets_provider = secrets.Secrets(
        session=db.session,
        config=db.config,
        secrets_provider=databricks_utils_helper.dbutils_wrapper()).clear_cache()

    cosmos_fixture.MockCosmosDBStreamReader.db_table_name = "my_db.mock_cosmos_db"

    return MyCosmosTable(db=db,
                         secrets_provider=secrets_provider,
                         stream_reader=cosmos_fixture.MockCosmosDBStreamReader)
