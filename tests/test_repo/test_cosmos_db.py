from jobsworthy import repo
from jobsworthy.util import secrets

from tests.shared import tables, cosmos_fixture, databricks_utils_helper


def test_default_spark_config_options(test_db):
    my_cosmos_table = cosmos_table_setup(test_db, MyCosmosTable)

    opts = my_cosmos_table._spark_config_options()

    assert opts == {'spark.cosmos.accountEndpoint': 'cosmos_endpoint',
                    'spark.cosmos.accountKey': 'from: my_domain.my_data_product_name/my_cosmos_account_key generates a-secret',
                    'spark.cosmos.database': 'cosmos_db_name',
                    'spark.cosmos.container': 'cosmos_container_name'}


def test_extended_spark_options(test_db):
    my_cosmos_table = cosmos_table_setup(test_db, CosmosTableWithExtends)

    opts = my_cosmos_table._spark_config_options()

    assert opts == {'spark.cosmos.accountEndpoint': 'cosmos_endpoint',
                    'spark.cosmos.accountKey': 'from: my_domain.my_data_product_name/my_cosmos_account_key generates a-secret',
                    'spark.cosmos.database': 'cosmos_db_name',
                    'spark.cosmos.container': 'cosmos_container_name',
                    "spark.cosmos.read.inferSchema.enabled": "true",
                    "spark.cosmos.write.strategy": "ItemOverwrite",
                    "spark.cosmos.read.partitioning.strategy": "Default",
                    "spark.cosmos.changeFeed.mode": "Incremental"}


def test_reads_from_stream(test_db):
    mock_cosmos_table(test_db)
    my_cosmos_table = cosmos_table_setup(test_db, MyCosmosTable)

    df = my_cosmos_table.read_stream()

    assert df.isStreaming


def test_reads_from_stream_with_read_schema(test_db):
    mock_cosmos_table(test_db)
    my_cosmos_table = cosmos_table_setup(test_db, CosmosTableWithExtends)

    df = my_cosmos_table.read_stream()

    assert df.isStreaming


#
# Helpers
#
class MyCosmosTable(repo.CosmosDb):
    pass


class CosmosTableWithExtends(repo.CosmosDb):
    additional_spark_options = {"spark.cosmos.read.inferSchema.enabled": "true",
                                "spark.cosmos.write.strategy": "ItemOverwrite",
                                "spark.cosmos.read.partitioning.strategy": "Default",
                                "spark.cosmos.changeFeed.mode": "Incremental"}

    def read_schema_as_dict(self):
        return {'type': 'struct', 'fields': [{'name': 'id', 'type': 'string', 'nullable': True, 'metadata': {}},
                                             {'name': 'isDeleted', 'type': 'string', 'nullable': True, 'metadata': {}},
                                             {'name': 'name', 'type': 'string', 'nullable': True, 'metadata': {}},
                                             {'name': 'pythons', 'type': {'type': 'array',
                                                                          'elementType': {'type': 'struct', 'fields': [
                                                                              {'name': 'id', 'type': 'string',
                                                                               'nullable': True, 'metadata': {}}]},
                                                                          'containsNull': True}, 'nullable': True,
                                              'metadata': {}},
                                             {'name': 'season', 'type': 'string', 'nullable': True, 'metadata': {}}]}


def mock_cosmos_table(db):
    df = tables.my_table_df(db)
    (df.write
     .format('delta')
     .mode("append")
     .saveAsTable('my_db.mock_cosmos_db'))


def cosmos_table_setup(db, table_cls):
    secrets_provider = secrets.Secrets(
        session=db.session,
        config=db.config,
        secrets_provider=databricks_utils_helper.dbutils_wrapper()).clear_cache()

    cosmos_fixture.MockCosmosDBStreamReader.db_table_name = "my_db.mock_cosmos_db"

    return table_cls(db=db,
                     secrets_provider=secrets_provider,
                     stream_reader=cosmos_fixture.MockCosmosDBStreamReader)
