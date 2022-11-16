from tests.shared import spark_test_session

from pyspark.sql import functions as F
from jobsworthy.util import logger
from jobsworthy import performance
from jobsworthy import repo


class MyPerformanceRepo(performance.base_repo()):
    table_name = 'my_performance_table'

def setup_module():
    pass


def it_persists_the_observer_to_hive_using_emit(job_cfg_fixture, test_db):
    db = repo.spark_db.Db(session=spark_test_session.create_session(), job_config=job_cfg_fixture)

    table = performance.performance_table_factory(performance_repo=MyPerformanceRepo, db=db)

    performance.new_correlation("1", "2022-10-20T00:00:00Z")

    logs_performance()

    performance.write_log_to_db(table)

    cols = [F.col('run'),
            F.col('time'),
            F.col('counter'),
            F.col('delta_t')]

    df = (table.read()
               .withColumn("delta_t", F.element_at(F.col("perfmetrics").getItem("logs_performance"), 1))
               .select(cols))

    assert df.count() == 1

    row = df.collect()[0]

    assert row.run == "1"
    assert row.time == "2022-10-20T00:00:00Z"
    assert not row.counter
    assert row.delta_t

def it_resets_the_performance_metrics_to_default():
    performance.new_correlation("1", "2022-10-20T00:00:00Z")

    performance.new_correlation("1", "2022-10-20T00:00:00Z")
    logs_performance()

    assert [k for k in performance.performance_log().performance.keys()] == ['default', '1']

    performance.reset_logs()

    assert performance.performance_log().performance == {'default': {'time': None, 'counter': None, 'metrics': {}}}



#
# Helpers
#
@logger.with_perf_log(perf_log_type='fn', name='logs_performance', callback=performance.perf_log_callback)
def logs_performance():
    return True
