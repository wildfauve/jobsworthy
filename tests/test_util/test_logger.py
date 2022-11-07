from jobsworthy.util import logger, singleton


def test_log_level_with_kwargs(mocker):
    Spy().clear()
    mocker.patch('jobsworthy.util.logger.logger', Spy)
    logger.info(msg="msg", ctx={'key': "value"})

    assert Spy().logs == [({'ctx': {'key': 'value'}, 'status': 'ok'}, 'msg')]


def test_log_level_with_args(mocker):
    Spy().clear()
    mocker.patch('jobsworthy.util.logger.logger', Spy)
    logger.info("msg", None, 'ok', {'key': "value"})

    assert Spy().logs == [({'ctx': {'key': 'value'}, 'status': 'ok'}, 'msg')]


def test_perf_decorator(mocker):
    Spy().clear()
    mocker.patch('jobsworthy.util.logger.logger', Spy)

    with_perf_log_decorator()

    logs = Spy().logs

    assert logs[0][0]['ctx']['fn'] == "with_perf_log_decorator"
    assert logs[0][1] == "PerfLog"


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
@logger.with_perf_log()
def with_perf_log_decorator():
    return True


class LogPerformance(singleton.Singleton):
    metrics = []


class Spy(singleton.Singleton):
    logs = []

    def info(self, meta, msg):
        self.logs.append((meta, msg))
        pass

    def clear(self):
        self.logs = []


def perf_log_callback(name, delta):
    LogPerformance().metrics.append((name, delta))


@logger.with_perf_log(perf_log_type="function", name="test.function_with_logging", callback=perf_log_callback)
def function_with_logging():
    return True
