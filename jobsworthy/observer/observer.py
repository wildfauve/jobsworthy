from typing import List, Tuple, Callable, Protocol, Union, Optional, Any
import pendulum
from rdflib import Namespace, URIRef
from uuid import uuid4
from functools import reduce
import pyspark
from pyspark.sql import dataframe

from jobsworthy.observer.domain import error, table
from jobsworthy.observer import repo
from jobsworthy import repo as hive_repo
from jobsworthy.util import monad, validate, logger
from jobsworthy.observer.domain import metrics
from jobsworthy import structure as S


class Observable:
    sfo_lin = Namespace("https://example.nz/ontology/Lineage/")

    def __init__(self):
        pass

    @staticmethod
    def coerce_uri(uri: URIRef) -> Optional[str]:
        if isinstance(uri, Namespace):
            return str(uri)
        if isinstance(uri, URIRef):
            return Observable.uriref_to_str(uri)
        if hasattr(uri, 'toPython'):
            return uri.toPython()
        return uri if isinstance(uri, str) else None

    @staticmethod
    def uriref_to_str(ref: URIRef) -> str:
        return ref.toPython()


class DataSet(Observable):
    type_of = None
    dataset_namespace = None

    @classmethod
    def namespace(cls, namespace):
        cls.dataset_namespace = namespace
        return cls

    def namespace_uri(self):
        if not isinstance(self.dataset_namespace, Namespace):
            raise error.ObserverConfigError("Namespace not configured and not configured with correct type")
        return self.dataset_namespace



class Hive(DataSet):
    pass


class EventTable(DataSet):
    pass


class ObjectStore(DataSet):
    pass


class HiveTable(Hive):
    type_of = Observable.sfo_lin.HiveTable

    def __init__(self, table_name, fully_qualified_name):
        self.table_name = table_name
        self.fully_qualified_name = fully_qualified_name

    def dataset_identity(self):
        return self.namespace_uri()

    def identity(self):
        return self.dataset_identity().term(self.fully_qualified_name)

    def to_props(self):
        return (
            self.coerce_uri(self.identity()),
            self.coerce_uri(self.type_of),
            self.fully_qualified_name,
            self.table_name
        )


class CosmosEventTable(EventTable):
    type_of = Observable.sfo_lin.CosmosEventTable

    def __init__(self, table_name, fully_qualified_name):
        self.table_name = table_name
        self.fully_qualified_name = fully_qualified_name

    def dataset_identity(self):
        return self.namespace_uri()

    def identity(self):
        return self.dataset_identity().term(self.fully_qualified_name)

    def to_props(self):
        return (
            self.coerce_uri(self.identity()),
            self.coerce_uri(self.type_of),
            self.fully_qualified_name,
            self.table_name
        )



class ObjectStoreFile(ObjectStore):
    type_of = Observable.sfo_lin.AzureDataLakeStoreFile

    def __init__(self, location):
        self.uuid = str(uuid4())
        self.location = location

    def identity(self):
        return self.dataset_namespace.term(self.uuid)

    def to_props(self):
        return (
            self.coerce_uri(self.identity()),
            self.coerce_uri(self.type_of),
            self.location,
            None  # name
        )


class Job(Observable):
    job_namespace = None

    @classmethod
    def namespace(cls, ns):
        cls.job_namespace = ns

    def namespace_uri(self):
        if not isinstance(self.job_namespace, Namespace):
            raise error.ObserverConfigError("Namespace not configured and not configured with correct type")
        return self.job_namespace


class SparkJob(Job):
    type_of = Observable.sfo_lin.SparkJob


class Run(Job):
    """
    A Spark job has the ability to process 0 or more batches or streams.  Each dataset process is an instance of a "Run".
    Each dataset contains trace (causal id) data.  While a Spark job may process many files, each file is not
    necessarily from the same trace, hence why the batch is the unit of work.
    """

    def __init__(self, job, parent_observer=None):
        self.uuid = str(uuid4())
        self.trace = None
        self.job = job
        self.parent_observer = parent_observer
        self.current_state = None
        self.state_transitions = []
        self.inputs = []
        self.outputs = []
        self.metrics = {}
        self.start_time = pendulum.now(tz='UTC')
        self.end_time = None
        self.input = None

    def __key(self):
        return (self.identity,)

    def __eq__(self, other):
        return self.__key == other.__key

    def __hash__(self):
        return hash(self.__key())

    def add_trace(self, trace: Union[str, URIRef]):
        self.trace = trace
        return self

    def trace_id(self):
        if not self.trace:
            return None
        return self.coerce_uri(self.trace)

    def start(self):
        self.start_time = pendulum.now('UTC')
        return self

    def complete(self):
        self.end_time = pendulum.now('UTC')
        return self

    def complete_and_emit(self):
        self.end_time = pendulum.now('UTC')
        self.parent_observer.emit([self])
        return self

    def job_identity(self):
        return self.job.namespace_uri()

    def identity(self):
        return self.job_identity().term(self.uuid)

    def has_input(self, dataset: DataSet):
        self.inputs.append(dataset)
        return self

    def has_output(self, dataset: DataSet):
        self.outputs.append(dataset)
        return self

    def with_state_transition(self, transition_fn: Callable):
        """
        The Transition fn is provided by the caller, and is expected to return a tuple containing
        (new_state, transition_event).  It takes a current state in the format which is understood by the transition
        function.

        The state returned must either be a string or a ENUM-like object that has a value attribute which returns a
        string.
        """
        new_current_state, event = transition_fn(self.current_state)
        self.state_transitions.append({'from': self.current_state, 'with': event, 'to': new_current_state})
        self.current_state = new_current_state
        return self

    def inputs_as_props(self, row):
        cell = row.cell_from_schema_name("hasInputs").values(
            identity=None,
            props={"listOfInput": [inp.to_props() for inp in self.inputs]})
        return cell

    def outputs_as_props(self, row):
        cell = row.cell_from_schema_name("hasOutputs").values(
            identity=None,
            props={"listOfOutputs": [output.to_props() for output in self.outputs]})
        return cell

    def metric_factory(self, metric_type, name, **kwargs):
        """
        Creates and returns an initialised metric.  The metric is saved against the run, and can be used outside it.
        If the metric already exists by name, it is returned rather than recreated.

        :param metric_type:  The type must be supported by a create_<metric_type> function on the provider.
        :param name: name of the metric
        :param kwargs: Must be valid additonal args to pass to the metric's create function.
        :return:
        """
        if self.metrics.get(name, None):
            return self.metrics.get(name)
        metric = getattr(self.metric_provider(), "create_{}".format(metric_type))(**{**{'name': name}, **kwargs})
        self.metrics[name] = metric
        self.all_metrics_dimensions()
        return metric

    def metric_by_name(self, name):
        return self.metrics.get(name, None)

    def metric_provider(self):
        return metrics.metric_provider()

    def all_metrics_dimensions(self):
        return reduce(self.metric_dimensions, self.metrics.items(), {})

    def metric_dimensions(self, acc, metric: Tuple[str, Callable]):
        metric_name, metric_fn = metric
        acc.update({metric_name: metric_fn.dimensions})
        return acc

    def collect_metrics(self, row):
        cell = row.cell_from_schema_name("hasMetrics").values(
            identity=None,
            props={"listOfMetrics": [obj.to_json() for name, obj in self.metrics.items()]})
        return cell

    def build_rows(self, rows):
        return [tuple(cell.props for cell in row) for row in rows]

    def as_row(self, row):
        row.cell_from_schema_name('hasRunTime').values(
            identity=None,
            props={'timeLiteral': self.start_time.to_iso8601_string()}
        )

        _dim_month, dim_day = self.day_from_time(self.start_time)
        row.cell_from_schema_name('hasRunDateUTC').values(
            identity=None,
            props={'dateLiteral': dim_day}
        )

        row.cell_from_schema_name('run').values(
            identity=None,
            props={
                "id": self.coerce_uri(self.identity()),
                "type": self.coerce_uri(self.job.type_of),
                "jobId": self.coerce_uri(self.job_identity()),
                "traceId": self.coerce_uri(self.trace) if self.trace else None,
                "startTime": self.start_time.to_iso8601_string() if self.start_time else None,
                "endTime": self.end_time.to_iso8601_string() if self.end_time else None,
                "state": self.current_state if isinstance(self.current_state, str) else self.current_state.value}
        )
        self.inputs_as_props(row)
        self.outputs_as_props(row)
        self.collect_metrics(row)
        return row

    def day_from_time(self, runtime) -> Tuple[str, str]:
        time = self.safe_time_convert(runtime) >> self.safe_time_parser
        if time.is_left():
            return None, None
        return time.value.format("YYYY-MM"), time.value.to_date_string()

    @monad.monadic_try()
    def safe_time_convert(self, time_literal):
        return time_literal.isoformat()

    @monad.monadic_try()
    def safe_time_parser(self, time_str: str) -> monad.EitherMonad[pendulum.DateTime]:
        return pendulum.parse(time_str)

    def __str__(self):
        return """
        UUID: {uuid}
        Identity: {idt}
        JobIdentity: {jid}
        StartTime: {st}
        EndTime: {et}
        Trace: {sp}
        StateTransitions: {stt}
        """.format(uuid=self.uuid,
                   idt=self.coerce_uri(self.identity()),
                   jid=self.job_identity(),
                   st=self.start_time,
                   et=self.end_time,
                   sp=self.trace,
                   stt=self.state_transitions)

    def serialise(self):
        """
        Required for the logger interface.  Collects both observer and run info.
        """
        obs_data = self.parent_observer.serialise() if self.parent_observer else {}
        return {**{'trace_id': self.trace_id()}, **obs_data}


class Emitter(Protocol):

    def __init__(self, session: pyspark.sql.session):
        ...

    def emit(self) -> monad.EitherMonad:
        ...

    def read(self) -> Optional[Any]:
        ...

    def filter_by_inputs_run_state(self, run_state: str, input_locations: List[str]) -> dataframe.DataFrame:
        ...


class ObserverHiveEmitter(Emitter):

    def __init__(self, db: hive_repo.Db):
                 # session,
                 # job_config,
                 # db_naming_cls: hive_repo.DbNamingConventionProtocol = hive_repo.DbNamingConventionCallerDefined):
        self.emitted_map = set()
        if not self.session_is_spark_session(db.session):
            raise error.ObserverConfigError('Session provided is not a Spark Session.  Reconfigure Hive Emitter')
        self.repo = repo.REPO(db)
        # self.repo = repo.REPO(repo.DB(session=session,
        #                               job_config=job_config,
        #                               naming_convention=db_naming_cls))

    @monad.monadic_try(error_cls=error.ObserverError)
    def emit(self, table, runs: List[Run]):
        unemitted_runs = set(runs) ^ self.emitted_map

        logger.info('jobsworth:observer:emit', ctx={
            'table': self.repo.db_table_name(),
            'countOfRunsToEmit': len(unemitted_runs)
        })

        result = self.repo.try_write_append(df=self.create_df(table, unemitted_runs))

        if result.is_left():
            logger.info('jobsworth:observer:emit UPSERT ERROR', ctx=result.error().error())
        self.emitted_map.update(unemitted_runs)
        return result

    def read(self):
        return self.repo.read()

    def runs_to_rows(self, table, runs):
        return [run.as_row(table.row_factory()) for run in runs]

    def build_rows(self, table, runs):
        return [row.build_ordered_row_values() for row in self.runs_to_rows(table, runs)]

    def all_rows_ok(self, rows):
        return all(map(S.all_cells_ok, rows))

    def create_df(self, table, runs: List[Run]):
        return self.repo.create_df(self.build_rows(table, runs), table.hive_schema())

    def filter_by_inputs_run_state(self, run_state: str, input_locations: List[str]) -> dataframe.DataFrame:
        return self.repo.filter_by_inputs_run_state(run_state, input_locations)

    def session_is_spark_session(self, session):
        return hasattr(session, 'createDataFrame')


class ObserverNoopEmitter(Emitter):

    @monad.monadic_try(error_cls=error.ObserverError)
    def emit(self, table, runs: List[Run]):
        pass

    def read(self):
        return None

    def filter_by_inputs_run_state(self, run_state: str, input_locations: List[str]) -> dataframe.DataFrame:
        return []


class Observer(Observable):
    """
    An Observer observes the individual Job run.  It holds the run's identity, but does not mediate the components
    of the run.
    """

    def __init__(self, env: str, job: Job, emitter: Emitter):
        self.env = env
        self.job = job
        self.trace_id = str(uuid4())
        self.emitter = emitter
        self.runs = []
        self.table = table.observer_table_factory()

    def emit(self, runs: List[Run] = []):
        if not self.emitter:
            logger.info('jobsworth:observer:emit: WARNING: No Observer Emitter Configured')
            return self
        if runs:
            self.emitter.emit(self.table, runs)
        else:
            self.emitter.emit(self.table, self.runs)
        return self

    def read(self):
        return self.emitter.read()

    def filter_by_inputs_run_state(self, run_state: str, input_locations: List[str]) -> dataframe.DataFrame:
        return self.emitter.filter_by_inputs_run_state(run_state=run_state, input_locations=input_locations)

    def observer_identity(self):
        return self.job.namespace_uri()

    def identity(self):
        return self.observer_identity().term(self.trace_id)

    def run_factory(self, run):
        observable_run = run(job=self.job, parent_observer=self)
        self.runs.append(observable_run)
        logger.info(msg='observer:run_factory', ctx={'runId': observable_run.identity().toPython(),
                                                     'numberOfRuns': len(self.runs)})
        return observable_run

    def serialise(self):
        """
        Required for the logger interface
        """
        return {'env': self.env,
                'trace_id': self.coerce_uri(self.identity()),
                'time': pendulum.now().to_iso8601_string()}


def observer_factory(env: str, job: Job, emitter: Emitter) -> Observer:
    return Observer(env=env, job=job, emitter=emitter)


def define_namespace(cls, uri: str) -> None:
    if not cls in [SparkJob, Hive, ObjectStore, EventTable]:
        raise error.ObserverConfigError("Namespace must be configured on SparkJob, Hive, or ObjectStore")
    if not validate.valid_uri(uri):
        raise error.ObserverConfigError("Namespace must be a valid URI")
    cls.namespace(ns(uri))


def ns(uri: str) -> Namespace:
    return Namespace(uri)


def uri_ref(uri: str) -> URIRef:
    return URIRef(uri)
