import pytest
from dataclasses import dataclass

from pyspark.sql import functions as F
from jobsworthy import spark_job
from jobsworthy import repo, model

from tests.shared import tables, config_for_testing

"""
#
# Run the following in a notebook to see how the stream API works using the repo interfaces.
#

from jobsworthy import spark_job
from jobsworthy.repo import spark_db, hive_repo
from pyspark.sql import functions as F


def job_config():
    return (spark_job.JobConfig(data_product_name='testing_dataproduct',
                             domain_name='testing_domain',
                             service_name='test_service')
            .configure_hive_db(db_name='testdb',
                               db_file_system_path_root='/domains/testing_domain/data_products/test_dataproduct'))


db = spark_db.Db(session=spark, job_config=job_config())


class TestTable1(hive_repo.HiveRepo):
    table_name = 'test_table_1'
    table_properties = [
        hive_repo.TableProperty('org:spark:table:schema:version', '0.0.1')
    ]

    def identity_merge_condition(self, name_of_baseline, update_name):
        return f'{name_of_baseline}.id = {update_name}.id'


class TestTable2(hive_repo.HiveRepo):
    table_name = 'test_table_2'
    table_properties = [
        hive_repo.TableProperty('org:spark:table:schema:version', '0.0.1')
    ]


my_table_1 = TestTable1(db=db)
my_table_2 = TestTable2(db=db,
                        reader=hive_repo.HiveTableReader,
                        stream_writer=hive_repo.StreamHiveWriter)

in_df = spark.read.json('/FileStore/tests/testing_domain/table1_rows.json', multiLine=True, prefersDecimal=True)

my_table_1.create(in_df)

stream = my_table_1.read_stream()

df = stream.withColumn('onStream', F.lit('true'))

my_table_2.write_stream(df, ('name',))

my_table_2.await_termination()

table_2_df = my_table_2.read()

"""


def test_fluent_streaming_api(test_db, from_table, to_table):
    from_table.write_append(tables.my_table_df(test_db))

    streamer = (model.Streamer().stream_from(from_table)
                .stream_to(to_table)
                .with_transformer(transform_fn))

    result = streamer.run()

    assert result.is_right()

    table_2_df = to_table.read()

    assert "onStream" in table_2_df.columns
    assert table_2_df.rdd.getNumPartitions() == 2


def test_delta_merge_write_stream(test_db, from_table, to_table):
    from_table.write_append(tables.my_table_df(test_db))

    streamer = (model.Streamer().stream_from(from_table)
                .stream_to(table=to_table, write_type=model.StreamWriteType.UPSERT)
                .with_transformer(transform_fn))

    result = streamer.run()

    assert result.is_right()


def test_multi_streamer_with_1_stream(test_db, from_table, to_table):
    from_table.write_append(tables.my_table_df(test_db))

    streamer = (model.MultiStreamer().stream_from(from_table)
                .with_stream_to_pair(model.StreamToPair().stream_to(table=to_table,
                                                                    write_type=model.StreamWriteType.APPEND)
                                     .with_transformer(transform_fn)))

    result = streamer.run()

    assert result.is_right()

    table_2_df = to_table.read()

    assert "onStream" in table_2_df.columns
    assert table_2_df.rdd.getNumPartitions() == 2

def test_multi_streamer_with_2_streams(test_db, from_table, to_table, alternate_to_table):
    from_table.write_append(tables.my_table_df(test_db))

    streamer = (model.MultiStreamer().stream_from(from_table)
                .with_stream_to_pair(model.StreamToPair().stream_to(table=to_table,
                                                                    write_type=model.StreamWriteType.APPEND)
                                     .with_transformer(transform_fn))
                .with_stream_to_pair(model.StreamToPair().stream_to(table=alternate_to_table,
                                                                    write_type=model.StreamWriteType.APPEND)
                                     .with_transformer(alternate_transform_fn))
                )

    result = streamer.run()

    assert result.is_right()

    table_2_df = to_table.read()
    table_3_df = alternate_to_table.read()

    assert table_2_df.columns == ['id', 'isDeleted', 'name', 'pythons', 'season', 'onStream']
    assert table_3_df.columns == ['id', 'isDeleted', 'name', 'pythons', 'season', 'alternateOnStream']

def test_multi_streamer_with_2_streams_upsert_append(test_db, from_table, to_table, alternate_to_table):
    from_table.write_append(tables.my_table_df(test_db))

    streamer = (model.MultiStreamer().stream_from(from_table)
                .with_stream_to_pair(model.StreamToPair().stream_to(table=to_table,
                                                                    write_type=model.StreamWriteType.UPSERT)
                                     .with_transformer(transform_fn))
                .with_stream_to_pair(model.StreamToPair().stream_to(table=alternate_to_table,
                                                                    write_type=model.StreamWriteType.APPEND)
                                     .with_transformer(alternate_transform_fn))
                )

    result = streamer.run()

    assert result.is_right()

    table_2_df = to_table.read()
    table_3_df = alternate_to_table.read()

    assert table_2_df.columns == ['id', 'isDeleted', 'name', 'pythons', 'season', 'onStream']
    assert table_3_df.columns == ['id', 'isDeleted', 'name', 'pythons', 'season', 'alternateOnStream']


def test_any_context_to_transformer(test_db, from_table, to_table):
    @dataclass
    class TransformContext:
        run_id: int

    from_table.write_append(tables.my_table_df(test_db))

    streamer = (model.Streamer().stream_from(from_table)
                .stream_to(to_table, ('name',))
                .with_transformer(transform_fn_with_ctx, run=TransformContext(run_id=1)))

    result = streamer.run()

    assert result.is_right()

    table_2_df = to_table.read()

    assert "onStream" in table_2_df.columns
    assert table_2_df.rdd.getNumPartitions() == 2


#
# Helpers
#
@pytest.fixture
def from_table(test_db):
    return tables.MyHiveTable(db=test_db)


@pytest.fixture
def to_table(test_db):
    return tables.MyHiveTable2(db=test_db,
                               reader=repo.DeltaFileReader,
                               stream_writer=repo.StreamFileWriter)


@pytest.fixture
def alternate_to_table(test_db):
    return tables.MyHiveTable3(db=test_db,
                               reader=repo.DeltaFileReader,
                               stream_writer=repo.StreamFileWriter)


def transform_fn(df):
    return df.withColumn('onStream', F.lit("true"))

def alternate_transform_fn(df):
    return df.withColumn('alternateOnStream', F.lit("true"))


def transform_fn_with_ctx(df, **kwargs):
    return (df.withColumn('onStream', F.lit("true"))
            .withColumn('run_id', F.lit(kwargs.get('run', None).run_id)))


def job_config():
    return (spark_job.JobConfig(data_product_name="my_data_product_name",
                                domain_name="my_domain",
                                service_name="my_service")
            .configure_hive_db(db_name="my_db",
                               db_file_system_path_root=config_for_testing.DB_FILE_SYSTEM_PATH_ROOT,
                               db_path_override_for_checkpoint=config_for_testing.CHECKPOINT_OVERRIDE))
