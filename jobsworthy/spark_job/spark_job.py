from typing import Tuple
import types
from importlib import import_module
from pathlib import Path
from pymonad.tools import curry
from inspect import getmembers, ismodule

from jobsworthy.util import singleton

def job(initialiser_module: str = None):
    """
    Job provides a decorator which wraps the execution of a spark job.  You use the decorator at the entry point of the job

    @spark_job.job(initialiser_module="tests.shared.initialisers")
    def execute(args=None, location_partitioner: Callable = date_partitioner) -> monad.EitherMonad[value.JobState]:
        pass

    The initialiser_module provided to the decorator is a module in import path format.  All non "__init__.py" modules
    in this module will be dynamically imported, and those which are decorated with @spark_job.register() will be
    executed before the job starts.  This is a great place to include any state, functions, etc, which need to be
    initialised before starting the job.

    Job does the following:
    + It calls the initialiser to run all the initialisations registered
    + It then invokes the job function with all args and kwargs.
    + At job completion it simply returns whatever the job function returned.

    """
    def inner(fn):
        def invoke(*args, **kwargs):
            mod = kwargs.get('initialiser_module', None) or initialiser_module

            # if mod:
            #     initialisation_importer(mod)

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


# def initialisation_importer(initialiser_mod: types.ModuleType):
#     list(map(import_initialiser, getmembers(initialiser_mod, ismodule)))
#
# def files_in_init_path(path):
#     return list(path.glob("**/*.py"))
#
# def import_initialiser(module: Tuple[str, types.ModuleType]) -> None:
#     pass
#     # breakpoint()
#     # if "__" in file.name:
#     #     return None
#     # full_module = f"{module}.{file.name.replace('.py', '')}"
#     # logger.info(msg=f"JobsWorth:import_initialiser, importing initialiser: {full_module}")
#     # import_module(full_module)
#     # pass

