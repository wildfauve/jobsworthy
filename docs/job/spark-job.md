# Spark Job Module

Job provides a decorator which wraps the execution of a spark job. You use the decorator at the entry point for the job.
At the moment it performs 1 function; calling all the registered initialisers.

```python
from jobsworthy import spark_job

@spark_job.job()
def execute(args=None) -> monad.EitherMonad[value.JobState]:
    pass
```

To register initialisers (to be run just before the job function is called) do the following.

```python
from jobsworthy import spark_job

@spark_job.register()
def some_initialiser():
    ...
```

The initialisers must be imported before the job function is called; to ensure they are registered. To do that, either
import them directly in the job module, or add them to a module `__init__.py` and import the module.


## Simple Streaming Decorator

Where the job adopts a streaming strategy which is base on a single input and output table, and a single transformation function, this decorator reduces job and command boilerplate.  It takes advantage of the streaming abstractions provided by `model.Streamer` class (see [the docs](../model/model.md)) to provide a 3 step streaming pipeline (read stream, transform, and write stream).  The `Streamer` also uses monadic try behaviour to capture unplanned exceptions.

This provides a way to get started with a streaming job before elaborating the architecture.

Its called simple, because there are only a small number of configuration points:
+ `from_table`.  A Callable which returns the table to stream from.  The returned table must be an instance of a `SparkRepo`.
+ `from_table_options`.  A set of reader option switches supported by `repo.ReaderSwitch` which controls some of the behaviours of reading from a stream.  For instance, to read using a schema defined by the repo, specify `repo.ReaderSwitch.READ_STREAM_WITH_SCHEMA_ON` 
+ `to_table`. A Callable which returns the table to stream to.  The returned table must be an instance of a `SparkRepo`.
+ `transform_fn`.  A Callable which takes a DF and returns a transformed DF.
+ `write_type`. An Enum declaring the type of stream write behaviour to execute on the `to_table`.  
  + `model.StreamWriteType.APPEND`.  Delta Append operation.
  + `model.StreamWriteType.UPSERT`.  Delta Upsert operation.  
+ `options`.  An optional List of `repo.SparkOption` which provides options to included on the spark session when writing the stream.  For example, to assert `{'mergeSchema': 'true'}` use `[repo.SparkOption.MERGE_SCHEMA]`.

The decorator executes the streaming pipeline and returns an instance of the `model.StreamState` value object (wrapped in a left or right monad) to the decorated function as the result argument containing the following properties:
+ `stream_configuration`: The model.Streamer object created to configure the stream.
+ `stream_input_dataframe`:  The DF used as input into the stream (the read of the to_table)
+ `stream_transformed_dataframe`:  The DF generated as output from the transformation
+ `error`:  An optional error object subclassed from Exception.

If using `@spark_job.job()` this should is the 1st decorator as it performs initialisations. 

```python
from jobsworthy import spark_job

@spark_job.simple_streaming_job(from_table=from_table_function,
                                from_reader_options={repo.ReaderSwitch.READ_STREAM_WITH_SCHEMA_ON},
                                to_table=to_table_function,
                                transformer=transform_fn,
                                write_type=model.StreamWriteType.APPEND,
                                options=None)
@spark_job.job()
def run_simple_streaming_job(result):
  if result.is_right():
    print("it worked")
  else:
    print(f"it failed with {result.error().error.message}")

```

The streaming configuration is also simplified because it does not perform any observer functions.  Nor does it perform any performance logging using streaming.

Note also, it consumes any arguments provided to the job and ignores them.