from pyspark.sql import functions as F
from jobsworthy import spark_job, repo, model
from jobsworthy.util import singleton, error

import pytest

from tests.shared import init_state_spy, initialisers, tables


class RepoState(singleton.Singleton):
    def db(self, test_db, dont_write_to_table: bool = False):
        self.test_db = test_db
        self.dont_write_to_table = dont_write_to_table

    def from_table(self):
        from_table = tables.MyHiveTable(db=self.test_db, stream_reader=repo.DeltaStreamReader)
        if self.dont_write_to_table:
            return from_table
        from_table.write_append(tables.my_table_df(self.test_db))
        return from_table

    def to_table(self):
        return tables.MyHiveTable2(db=self.test_db,
                                   reader=repo.DeltaFileReader,
                                   stream_writer=repo.StreamFileWriter)

    def to_table_with_error_writer(self):
        return tables.MyHiveTable2(db=self.test_db,
                                   reader=repo.DeltaFileReader,
                                   stream_writer=self.__class__)

    def write(self, *args, **kwargs):
        """
        The fake stream_writer function called by HiveRepo
        """
        raise Exception("Boom!")


def transform_fn(df):
    return df.withColumn('onStream', F.lit("true"))


def transform_fn_with_raise(_df):
    raise Exception("Boom!")

def it_runs_the_initialiser():
    run_dummy_job()

    assert init_state_spy.InitState().state == ['a-thing-1', 'a-thing-2']


def it_runs_a_streaming_job(test_db):
    RepoState().db(test_db)
    result = run_simple_streaming_job()

    assert result.is_right()

    table_2_df = RepoState().to_table().read()

    assert table_2_df.count() == 2

    expected = ['The Spanish Inquisition', 'The Piranha Brothers']

    assert [column.name for column in table_2_df.select(table_2_df.name).collect()] == expected


def it_runs_a_streaming_job_with_own_args(test_db):
    RepoState().db(test_db)
    result = run_simple_streaming_job("some-arg", some_kw="kwarg")

    assert result.is_right()


def it_runs_a_streaming_job_and_returns_value_to_the_caller(test_db):
    RepoState().db(test_db)
    result = run_simple_streaming_job()

    assert isinstance(result.value, model.StreamState)


def it_fails_with_stream_read_error(test_db):
    RepoState().db(test_db, dont_write_to_table=True)
    result = run_simple_streaming_job()

    assert result.is_left()
    assert isinstance(result.error().error, error.RepoStreamReadError)


def it_fails_with_transform_failure(test_db):
    RepoState().db(test_db)
    result = run_simple_streaming_job_with_transform_error()

    assert result.is_left()
    assert isinstance(result.error().error, error.StreamerTransformerError)


def it_fails_with_write_failure(test_db):
    RepoState().db(test_db)
    result = run_simple_streaming_job_with_write_error()

    assert result.is_left()
    assert isinstance(result.error().error, error.RepoWriteError)



@spark_job.job(initialiser_module=initialisers)
def run_dummy_job():
    return True


@spark_job.simple_streaming_job(from_table=RepoState().from_table,
                                from_reader_options={repo.ReaderSwitch.READ_STREAM_WITH_SCHEMA_OFF},
                                to_table=RepoState().to_table,
                                transformer=transform_fn,
                                write_type=model.StreamWriteType.APPEND,
                                options=None)
@spark_job.job(initialiser_module=initialisers)
def run_simple_streaming_job(result):
    return result


@spark_job.simple_streaming_job(from_table=RepoState().from_table,
                                to_table=RepoState().to_table,
                                transformer=transform_fn_with_raise,
                                write_type=model.StreamWriteType.APPEND,
                                options=None)
@spark_job.job(initialiser_module=initialisers)
def run_simple_streaming_job_with_transform_error(result):
    return result


@spark_job.simple_streaming_job(from_table=RepoState().from_table,
                                to_table=RepoState().to_table_with_error_writer,
                                transformer=transform_fn,
                                write_type=model.StreamWriteType.APPEND,
                                options=None)
@spark_job.job(initialiser_module=initialisers)
def run_simple_streaming_job_with_write_error(result):
    return result
