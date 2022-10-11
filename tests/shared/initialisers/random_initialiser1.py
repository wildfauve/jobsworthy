from jobsworth import spark_job

from tests.shared import init_state_spy


@spark_job.register()
def init_thing():
    init_state_spy.InitState().add_state("a-thing-1")
