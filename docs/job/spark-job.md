# Spark Job Module

Job provides a decorator which wraps the execution of a spark job. You use the decorator at the entry point for the job.
At the moment it performs 1 function; calling all the registered initialisers.

```python
from jobsworthy import spark_job
```

```python
@spark_job.job()
def execute(args=None) -> monad.EitherMonad[value.JobState]:
    pass
```

To register initialisers (to be run just before the job function is called) do the following.

```python
@spark_job.register()
def some_initialiser():
    ...
```

The initialisers must be imported before the job function is called; to ensure they are registered. To do that, either
import them directly in the job module, or add them to a module `__init__.py` and import the module.

