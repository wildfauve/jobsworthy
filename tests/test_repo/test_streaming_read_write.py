import pytest
from pyspark.sql import functions as F

from jobsworthy import repo
from jobsworthy.util import error

from tests.shared import tables


def test_read_write_streams_append_only(test_db):
    my_table = tables.MyHiveTable(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    my_table_2 = tables.MyHiveTable2(db=test_db,
                                     reader=repo.DeltaFileReader,
                                     stream_writer=repo.StreamFileWriter)

    stream = my_table.read_stream()

    df = stream.withColumn('onStream', F.lit("true"))

    my_table_2.write_stream_append(df)

    my_table_2.await_termination()

    table_2_df = my_table_2.read()

    assert "onStream" in table_2_df.columns


def test_write_stream_upserts(test_db):
    my_table = tables.MyHiveTable(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    my_table_2 = tables.MyHiveTable2(db=test_db,
                                     reader=repo.DeltaFileReader,
                                     stream_writer=repo.StreamFileWriter)

    # stream 1 # create 2 rows
    stream = my_table.read_stream()

    df = stream.withColumn('onStream', F.lit("true"))

    my_table_2.stream_write_via_delta_upsert(df)

    my_table_2.await_termination()

    table_2_df = my_table_2.read()

    assert table_2_df.count() == 2

    # Stream 2  Add 2 rows

    my_table.upsert(tables.my_table_df_new_rows(test_db))

    stream = my_table.read_stream()
    df = stream.withColumn('onStream', F.lit("true"))

    my_table_2.stream_write_via_delta_upsert(df)
    my_table_2.await_termination()

    in_df = my_table.read()
    out_df = my_table_2.read()

    assert in_df.count() == out_df.count()

    # Stream 3  Update 1 row

    my_table.upsert(tables.my_table_df_updated_row(test_db))

    stream = my_table.read_stream()
    df = stream.withColumn('onStream', F.lit("true"))
    my_table_2.stream_write_via_delta_upsert(df)
    my_table_2.await_termination()

    out_df = my_table_2.read()

    assert out_df.count() == 4


def test_cant_use_hive_stream_writer_in_test(test_db):
    my_table = tables.MyHiveTable(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    my_table_2 = tables.MyHiveTable2(db=test_db,
                                     reader=repo.DeltaFileReader,
                                     stream_writer=repo.StreamHiveWriter)

    stream = my_table.read_stream()

    df = stream.withColumn('onStream', F.lit("true"))

    with pytest.raises(error.RepoConfigError):
        my_table_2.write_stream_append(df)


def test_read_write_stream_to_df(test_db):
    my_table = tables.MyHiveTable(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    my_table_2 = tables.MyHiveTable2(db=test_db,
                                     reader=repo.DeltaFileReader,
                                     stream_writer=repo.StreamFileWriter)

    stream = my_table.read_stream()
    df = stream.withColumn('onStream', F.lit("true"))
    outDf = my_table_2.write_stream_temporary(df)

    assert "onStream" in outDf.columns
    assert outDf.rdd.getNumPartitions() == 2

# def test_delta_upsert_on_stream(test_db):
#     my_table = tables.MyHiveTable(db=test_db)
#     my_table.create(tables.my_table_df(test_db))
#
#     my_table_2 = tables.MyHiveTable2(db=test_db)
#
#     # Stream 1
#     stream = my_table.read_stream()
#     df = stream.withColumn('onStream', F.lit("true"))
#     result = my_table_2.delta_stream_upserter(df, "name", ("name",))
#
#     assert result.is_right()
#
#     in_df = my_table.read()
#     out_df = my_table_2.read()
#
#     assert in_df.count() == out_df.count()
#
#     # Stream 2
#
#     my_table.upsert(tables.my_table_df_new_rows(test_db), "name", "name")
#
#     stream = my_table.read_stream()
#     df = stream.withColumn('onStream', F.lit("true"))
#     my_table_2.delta_stream_upserter(df, "name", ("name",))
#
#     in_df = my_table.read()
#     out_df = my_table_2.read()
#
#     breakpoint()
#
#     assert in_df.count == out_df.count


# def test_delta_upsert_on_stream_without_partitioning(test_db):
#     my_table = MyHiveTable(db=test_db)
#     my_table.create(my_table_df(test_db))
#
#     my_table_2 = MyHiveTable2(db=test_db)
#
#     stream = my_table.read_stream()
#     df = stream.withColumn('onStream', F.lit("true"))
#     result = my_table_2.delta_stream_upserter(df)
#
#     assert result.is_right()
#
#     out_df = my_table_2.read()
#
#     assert "onStream" in out_df.columns
#     assert out_df.rdd.getNumPartitions() == 1
