from jobsworthy import repo
from jobsworthy.util import secrets

from tests.shared import tables, cosmos_fixture, databricks_utils_helper


def test_default_spark_config_options(test_db):
    my_cosmos_table = cosmos_table_setup(test_db, MyCosmosTable, ok_secret_provider(test_db))

    opts = my_cosmos_table.spark_config_options()

    assert opts.is_right()

    assert opts.value == {'spark.cosmos.accountEndpoint': 'cosmos_endpoint',
                          'spark.cosmos.accountKey': 'from: my_domain.my_data_product_name/my_cosmos_account_key generates a-secret',
                          'spark.cosmos.database': 'cosmos_db_name',
                          'spark.cosmos.container': 'cosmos_container_name'}


def test_default_spark_config_options_when_using_cosmos_connection_string(test_db_cosmos_connection_string):
    my_cosmos_table = cosmos_table_setup(test_db_cosmos_connection_string,
                                         MyCosmosTable,
                                         secret_provider_with_cosmos_connect_string(test_db_cosmos_connection_string))

    opts = my_cosmos_table.spark_config_options()

    assert opts.is_right()

    assert opts.value == {'spark.cosmos.accountEndpoint': 'https://example.co.nz:443/',
                          'spark.cosmos.accountKey': 'a-secret-acct-key===',
                          'spark.cosmos.database': 'cosmos_db_name',
                          'spark.cosmos.container': 'cosmos_container_name'}


def test_returns_left_when_secret_cant_be_found(test_db):
    mock_cosmos_table(test_db)
    my_cosmos_table = cosmos_table_setup(test_db, MyCosmosTable, secret_provider_without_job_config_secret(test_db))

    opts = my_cosmos_table.spark_config_options()

    err_msg = 'Secret does not exist with scope: my_domain.my_data_product_name and key: my_cosmos_account_key'

    assert opts.is_left()
    assert opts.error().message == err_msg


def test_extended_spark_options(test_db):
    my_cosmos_table = cosmos_table_setup(test_db, CosmosTableWithExtends, ok_secret_provider(test_db))

    opts = my_cosmos_table.spark_config_options()

    assert opts.is_right()

    assert opts.value == {'spark.cosmos.accountEndpoint': 'cosmos_endpoint',
                          'spark.cosmos.accountKey': 'from: my_domain.my_data_product_name/my_cosmos_account_key generates a-secret',
                          'spark.cosmos.database': 'cosmos_db_name',
                          'spark.cosmos.container': 'cosmos_container_name',
                          "spark.cosmos.read.inferSchema.enabled": "true",
                          "spark.cosmos.write.strategy": "ItemOverwrite",
                          "spark.cosmos.read.partitioning.strategy": "Default",
                          "spark.cosmos.changeFeed.mode": "Incremental"}


def test_reads_from_stream(test_db):
    mock_cosmos_table(test_db)
    my_cosmos_table = cosmos_table_setup(test_db, MyCosmosTable, ok_secret_provider(test_db))

    df = my_cosmos_table.read_stream()

    assert df.isStreaming


def test_fails_when_secret_is_not_found(test_db):
    mock_cosmos_table(test_db)
    my_cosmos_table = cosmos_table_setup(test_db, MyCosmosTable, secret_provider_without_job_config_secret(test_db))

    df = my_cosmos_table.read_stream()

    assert not df


def test_reads_from_stream_with_schema(test_db):
    mock_cosmos_table(test_db)
    my_cosmos_table = cosmos_table_setup(test_db, MyCosmosTable, ok_secret_provider(test_db))

    df = my_cosmos_table.read_stream()

    assert df.isStreaming


def test_reads_from_stream_with_read_schema(test_db):
    mock_cosmos_table(test_db)
    my_cosmos_table = cosmos_table_setup(test_db, CosmosTableWithExtends, ok_secret_provider(test_db))

    df = my_cosmos_table.read_stream()

    assert df.isStreaming


#
# Helpers
#
class MyCosmosTable(repo.CosmosDb):
    pass


class CosmosTableWithExtends(repo.CosmosDb):
    additional_spark_options = [
        repo.SparkOption.COSMOS_INFER_SCHEMA,
        repo.SparkOption.COSMOS_ITEM_OVERWRITE,
        repo.SparkOption.COSMOS_READ_PARTITION_DEFAULT,
        repo.SparkOption.COSMOS_CHANGE_FEED_INCREMENTAL
    ]

    def schema_as_dict(self):
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


def cosmos_table_setup(db, table_cls, secrets_provider_config):
    cosmos_fixture.MockCosmosDBStreamReader.db_table_name = "my_db.mock_cosmos_db"

    return table_cls(db=db,
                     secrets_provider=secrets_provider_config,
                     stream_reader=cosmos_fixture.MockCosmosDBStreamReader)


def ok_secret_provider(db):
    return build_secrets_provider(db)


def secret_provider_without_job_config_secret(db):
    return build_secrets_provider(db, {"blah":
                                           {'blah': 'a-secret'}
                                       })


def secret_provider_with_cosmos_connect_string(db):
    return build_secrets_provider(
        db,
        {"my_domain.my_data_product_name":
            {
                'my_cosmos_connection_string': 'AccountEndpoint=https://example.co.nz:443/;AccountKey=a-secret-acct-key===;'}})


def build_secrets_provider(db, secrets_to_load=None):
    return secrets.Secrets(
        session=db.session,
        config=db.config,
        secrets_provider=databricks_utils_helper.dbutils_wrapper(secrets_to_load)).clear_cache()
