from jobsworthy.util import singleton, logger
from jobsworthy.performance.repo import performance as repo
from functools import reduce


class PerfLogCapture(singleton.Singleton):
    count_col = 'counter'
    metrics_col = 'metrics'
    time_col = 'time'
    current_correlation = "default"
    performance = {current_correlation: {time_col: None, count_col: None, metrics_col: {}}}

    def add_count(self, ct):
        self.__class__.performance[self.__class__.current_correlation][self.__class__.count_col] = ct

    def add_delta(self, name, delta):
        if name in self.current_metric_location():
            self.__class__.performance[self.__class__.current_correlation][self.__class__.metrics_col][name].append(
                delta)
            return self
        self.__class__.performance[self.__class__.current_correlation][self.__class__.metrics_col][name] = [delta]
        return self

    def new_correlation(self, correlation_id, time_string):
        self.__class__.performance[correlation_id] = {
            self.__class__.time_col: time_string,
            self.__class__.metrics_col: {},
            self.__class__.count_col: None
        }
        self.__class__.current_correlation = correlation_id
        return self

    def current_metric_location(self):
        return self.__class__.performance[self.__class__.current_correlation][self.__class__.metrics_col]

    def to_data(self):
        return reduce(self.to_row, self.__class__.performance.items(), [])

    def to_row(self, acc, item):
        k, v = item
        if not v['metrics']:
            # ignore default is there are no metrics
            return acc
        acc.append((k,
                    v[self.__class__.time_col],
                    v[self.__class__.count_col],
                    v[self.__class__.metrics_col]))
        return acc

    def reset(self):
        self.__class__.current_correlation = "default"
        self.__class__.performance = {
            self.__class__.current_correlation: {
                self.__class__.time_col: None,
                self.__class__.count_col: None,
                self.__class__.metrics_col: {}}}


def performance_log():
    return PerfLogCapture()

def perf_log_callback(name, delta):
    PerfLogCapture().add_delta(name, delta)
    pass


def append_number_of_units(units):
    PerfLogCapture().add_count(units)


def new_correlation(correlation_id, time_string):
    PerfLogCapture().new_correlation(correlation_id, time_string)


@logger.with_perf_log(perf_log_type="function", name="jobsworth:perf_log.write_log_to_db")
def write_log_to_db(table):
    table.emit_metrics(PerfLogCapture().to_data())
    pass

def reset_logs():
    PerfLogCapture().reset()

def performance_table_factory(performance_repo, db):
    return performance_repo(db=db)


def base_repo():
    return repo.PerformanceMetric
