from .util import singleton

def job():
    """
    Job provides a decorator which wraps the execution of a spark job.  You use the decorator at the entry point of the job

    @spark_job.job()
    def execute(args=None, location_partitioner: Callable = date_partitioner) -> monad.EitherMonad[value.JobState]:
        pass

    Job does the following:
    + It calls the initer to run all the initialisations registered
    + It then invokes the job function with all args and kwargs.
    + At job completion it simply returns whatever the job function returned.

    """
    def inner(fn):
        def invoke(*args, **kwargs):
            initialisation_runner()
            result = fn(*args, **kwargs)
            return result
        return invoke
    return inner


class Initialiser(singleton.Singleton):
    init_fns = []

    def add_initialiser(self, fn):
        self.init_fns.append(fn)

    def invoke_fns(self):
        [f() for f in self.init_fns]

def register():
    """
    Decorator for registering initialisers to be run prior to the main job execution.  Note that the module containing
    the initialiser must be imported before the job entry point is called.

    @spark_job.register()
    def session_builder():
        pass

    All registered initialisers are invoked, in the order of registration, by the job decorator.
    """
    def inner(fn):
        Initialiser().add_initialiser(fn=fn)
    return inner


def initialisation_runner():
    Initialiser().invoke_fns()
