from rdflib import URIRef, Namespace
import time_machine
import pendulum

from jobsworth.observer import observer

from tests.shared import spark_test_session


class RunOfMySparkJob(observer.Run):
    pass


class MyInputHiveTable(observer.HiveTable):
    pass


class MyOutputHiveTable(observer.HiveTable):
    pass


class MyOutputHiveTable2(observer.HiveTable):
    pass


def setup_module():
    observer.define_namespace(observer.Hive, 'https://example.nz/service/datasets/dataset/')
    observer.define_namespace(observer.SparkJob, 'https://example.nz/service/jobs/job/')
    observer.define_namespace(observer.ObjectStore, 'https://example.nz/service/datasets/batchFile/')


def it_creates_an_observable():
    emitter = observer.ObserverNoopEmitter()

    obs = observer.observer_factory("test", observer.SparkJob(), emitter)

    assert obs.observer_identity() == Namespace('https://example.nz/service/jobs/job/')


def it_created_a_run_from_the_observer():
    obs = create_obs()

    job_run = obs.run_factory(RunOfMySparkJob)

    assert "https://example.nz/service/jobs/job/" in str(job_run.identity())


def it_adds_a_run_input():
    job_run = create_run()

    input_table = MyInputHiveTable(table_name="myTable", fully_qualified_name="myDB.myTable")

    job_run.has_input(dataset=input_table)

    assert job_run.inputs[0].identity() == URIRef('https://example.nz/service/datasets/dataset/myDB.myTable')



def test_run_takes_multiple_inputs():
    job_run = create_run()

    job_run.has_input(dataset=observer.ObjectStoreFile(location="file_loc"))
    job_run.has_input(dataset=MyInputHiveTable(table_name="myInputTable1", fully_qualified_name="myDB.myOutputTable1"))

    assert len(job_run.inputs) == 2


def it_adds_multiple_run_outputs():
    job_run = create_run()

    output_table1 = MyOutputHiveTable(table_name="myOutputTable1", fully_qualified_name="myDB.myTable1")
    output_table2 = MyOutputHiveTable(table_name="myOutputTable2", fully_qualified_name="myDB.myTable2")

    job_run.has_output(dataset=output_table1)
    job_run.has_output(dataset=output_table2)

    outputs = set([output.identity() for output in job_run.outputs])

    expected_outputs = set([URIRef('https://example.nz/service/datasets/dataset/myDB.myTable1'),
                            URIRef('https://example.nz/service/datasets/dataset/myDB.myTable2')])

    assert outputs == expected_outputs


def it_generates_multiple_runs_from_the_same_observer():
    obs = create_obs()

    run1 = obs.run_factory(RunOfMySparkJob)
    run2 = obs.run_factory(RunOfMySparkJob)

    assert obs.runs == [run1, run2]


@time_machine.travel(pendulum.datetime(2022, 10, 21, 9, 0, tz='UTC'))
def it_builds_table_from_run(job_cfg_fixture):
    obs = create_obs_with_hive_emitter(job_cfg_fixture)
    job_run = create_full_run(None, obs)

    rows = obs.emitter.runs_to_rows(obs.table, [job_run])

    assert len(rows) == 1

    assert len(rows[0].cells) == 6

    run_time_cell, run_day, run_cell, inps_cell, outputs_cell, metrics_cell = rows[0].cells

    run_id, job_type, job_id, trace, t1, t2, state = run_cell.build()

    input_id, input_type, location, _ = inps_cell.build()[0]

    table_id, table_type, db_table_name, table_name = outputs_cell.build()[0]

    metrics = metrics_cell.build()

    assert "2022-10-21T09:00:00" in run_time_cell.build()
    assert "2022-10-21" in run_day.build()
    assert 'https://example.nz/service/jobs/job/' in run_id
    assert job_type == 'https://example.nz/ontology/Lineage/SparkJob'
    assert job_id == 'https://example.nz/service/jobs/job/'
    assert trace == 'https://example.com/service/jobs/job/trace_uuid'
    assert state == "STATE_COMPLETE"
    assert t1
    assert t2

    assert 'https://example.nz/service/datasets/batchFile/' in input_id
    assert input_type == 'https://example.nz/ontology/Lineage/AzureDataLakeStoreFile'
    assert location == "file_loc"

    assert table_id == 'https://example.nz/service/datasets/dataset/myDB.myOutputTable1'
    assert table_type == 'https://example.nz/ontology/Lineage/HiveTable'
    assert db_table_name == 'myDB.myOutputTable1'
    assert table_name == 'myOutputTable1'

    assert not metrics

def test_all_cells_valid(job_cfg_fixture):
    obs = create_obs_with_hive_emitter(job_cfg_fixture)
    job_run = create_full_run(None, obs)

    rows = obs.emitter.runs_to_rows(obs.table, [job_run])

    assert obs.emitter.all_rows_ok(rows)


def it_sets_trace_id():
    job_run = create_full_run()

    assert job_run.trace_id() == "https://example.com/service/jobs/job/trace_uuid"



#
#
#
def create_obs():
    emitter = observer.ObserverNoopEmitter()
    return observer.observer_factory("test", observer.SparkJob(), emitter)

def create_obs_with_hive_emitter(job_cfg_fixture):
    emitter = observer.ObserverHiveEmitter(session=spark_test_session.MockPySparkSession,
                                           job_config=job_cfg_fixture)

    return observer.observer_factory("test", observer.SparkJob(), emitter)


def create_run(obs=None):
    if obs:
        return obs.run_factory(RunOfMySparkJob)
    return create_obs().run_factory(RunOfMySparkJob)


def create_full_run(run=None, obs=None):
    if not run:
        run = create_run(obs)
    (run.start()
     .add_trace('https://example.com/service/jobs/job/trace_uuid')
     .has_input(dataset=observer.ObjectStoreFile(location="file_loc"))
     .has_input(dataset=MyInputHiveTable(table_name="myInputTable1", fully_qualified_name="myDB.myInputTable1"))
     .has_output(dataset=MyOutputHiveTable(table_name="myOutputTable1", fully_qualified_name="myDB.myOutputTable1"))
     .has_output(dataset=MyOutputHiveTable2(table_name="myOutputTable2", fully_qualified_name="myDB.myOutputTable2"))
     .with_state_transition(lambda _s: ("STATE_COMPLETE", "EVENT_COMPLETED"))
     .complete())

    return run
