# Model Module

## Streamer

The `Streamer` module provides a fluent streaming abstraction on top of 2 hive repos (the from and to repos). The
Streamer runs a pipeline as follows:

+ Read from the source repo.
+ Perform a transformation.
+ Write to the target repo. This is where the stream starts and uses pyspark streaming to perform the read, transform
  and write.
+ Wait for the stream to finish.

To setup a stream:

```python
from jobsworthy import model

streamer = (model.Streamer()
            .stream_from(from_table)
            .stream_to(to_table)
            .with_transformer(transform_fn))

# Execute the stream
result = streamer.run()

# When successful it returns the Streamer wrapped in a Right.
# When there is a failure, it returns the error (subtype of JobError) wrapped in a Left   

assert result.is_right()
```

Some transformation functions require data from outside the input table. You can configure the streamer with additional
transformation context by passing in kwargs on the `with_transformer`.

```python
from dataclasses import dataclass
from jobsworthy import model


@dataclass
class TransformContext:
    run_id: int


def transform_fn_with_ctx(df, **kwargs):
    ...


streamer = (model.Streamer()
            .stream_from(from_table)
            .stream_to(to_table)
            .with_transformer(transform_fn_with_ctx, run=TransformContext(run_id=1)))

```

When configuring the `stream_to` table, you can provide partition columns when writing the stream. Provide a tuple of
column names.

```python
streamer = model.STREAMER().stream_to(to_table, ('name',))
```
