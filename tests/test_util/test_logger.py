from jobsworth.util import logger, singleton


class LogPerformance(singleton.Singleton):
    metrics = []

def test_performance_log_calls_back():
    function_with_logging()

    metrics = LogPerformance().metrics

    assert len(metrics) == 1
    name, t = metrics[0]
    assert name == "test.function_with_logging"
    assert isinstance(t, float)


#
# Helpers
#
def perf_log_callback(name, delta):
    LogPerformance().metrics.append((name, delta))


@logger.with_perf_log(perf_log_type="function", name="test.function_with_logging", callback=perf_log_callback)
def function_with_logging():
    return True
