from jobsworthy import observer
from jobsworthy.util import logger, singleton


class RunOfMySparkJob(observer.Run):
    pass


def setup_module():
    observer.define_namespace(observer.SparkJob, 'https://example.nz/service/jobs/job/')


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


def test_log_with_observer(mocker):
    Spy().clear()
    mocker.patch('jobsworthy.util.logger.logger', Spy)
    obs = an_observer()
    logger.info(msg="msg", ctx={'result': "a-value"}, observer=obs)

    logged_ctx, _ = Spy().logs[0]

    assert logged_ctx['ctx'] == {'result': "a-value"}
    assert logged_ctx['trace_id'] == obs.identity().toPython()


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


def an_observer():
    emitter = observer.ObserverNoopEmitter()

    return observer.observer_factory("test", observer.SparkJob(), emitter)
