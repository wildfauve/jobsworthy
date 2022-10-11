from jobsworth import spark_job

from tests.shared import init_state_spy

def it_runs_the_initialiser():
    run_dummy_job()

    assert init_state_spy.InitState().state == ['a-thing-1', 'a-thing-2']


@spark_job.job(initialiser_module='tests.shared.initialisers')
def run_dummy_job():
    return True

