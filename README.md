# Jobsworth

A set of utility functions and classes to aid in build Spark jobs on Azure databricks.

## Job Configuration

## Spark Job Module

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

## Model Module

### Streamer

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

streamer = (model.STREAMER()
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

### Repository Module

The repo library offers a number of simple abstractions for managing Databrick/Spark databases and tables. It is by no
means an object-mapper. Rather its a few classes with some simple functions we have found useful when working with Hive
tables.

```python
from jobsworthy import repo
```

### SparkDB

`Db` is the base class representing a Hive Database. Once constructed it is provided to the hive table classes when they
are constructed.

`Db` takes a [spark session](#spark-session) and a [job config](#job-configuration).

```python
db = repo.Db(session=spark_test_session.create_session(), config=job_config())
```

When intialised it checks that the database (defined in the config) exists and creates it if it doesn't.

### Hive Table

The `HiveTable` class is an abstraction for a delta or hive table. Inheriting from this class provides a number of
helper and table management. It also provides common reading and writing functions (both stream and batch).

#### Basic Configuration

The most basic Hive table can look like this.

```python
class MyHiveTable(repo.HiveRepo):
    table_name = "my_hive_table"


# Initialise it with a SparkDb instance
db = repo.Db(session=spark_test_session.create_session(), job_config=job_cfg())
my_table = MyHiveTable(db=db)

# Create a dataframe from the table
df = MyHiveTable().read()  # => pyspark.sql.dataframe.DataFrame

# Append to the Table
df2 = db.session.read.json("tests/fixtures/table1_rows.json", multiLine=True, prefersDecimal=True)
my_table.write_append(df2)
```

#### Table Lifecycle Events

By default, tables are created when first written to (via an append, an upsert, or a streaming function). Usually tables
are creates as "managed", meaning that the table is added to the catalogue and its associated files are managed as a
unit (that is a drop_table() call will remove the catalogue entry and the files).

Table creation can be forced if required, using the following functions:

+ `create_as_unmanaged_delta_table`. Creates an unmanaged table at the location defined by the naming strategy.
+ `create_as_managed_delta_table`. Creates a managed table based on the configuration of the catalogue.

These functions can be called directly. Alternatively, they can be called as part of a callback event. The require that
a schema be defined for the table.

```python
class MyHiveTable(repo.HiveRepo):
    table_name = "my_hive_table"
    schema = T.StructType([T.StructField('id', T.StringType(), True)])


my_table = MyHiveTable(db=db)
my_table.create_as_managed_delta_table()
```

#### Table Schema

#### Partitioning and Pruning

#### Table Properties

Hive table properties provide a means of storing key value pairs in the table, which are retrievable as a dataframe. The
table property key is a URN, while the value is a string. To store more complex objects in the value, requires
serialisation into a string and interpretation outside the library.

Use `repo.TableProperty` class to declare properties. This class takes a number of key formats; a URN, URN without the
URN portion (as a shortcut) and a common table property using the repo.DataAgreementType enum.

The table properties are declared as a class property on the table. The repo module is then able to maintain those
properties on the table. The merging of properties is the responsibility of the table instance.
Calling `self.property_manager.merge_table_properties()` will explicitly merge the declared difference of the properties
defined in the table with the properties on the Hive Table itself.

When table properties are declared, and the Hive table is created (`create_as_unmanaged_delta_table()`) the table
proeprties are merged to the table.

The table instance can also use callbacks to call the merge_table_properties() function.

```python
# Table using a URN (without the 'urn:' prefix, which is added when merged to the table).
# The properties are merged when the table instance is created.  This is an idempotent operation.
class MyHiveTable1(repo.HiveRepo):
    table_name = "my_hive_table_3"

    table_properties = [
        repo.TableProperty("my_namespace:spark:table:schema:version", "0.0.1")
    ]

    def after_initialise(self, _result):
        self.property_manager.merge_table_properties()


# Table showing the use of a full URN, and the merge executed when the table is created.
class MyHiveTable2(repo.HiveRepo):
    table_name = "my_hive_table_2"

    table_properties = [
        repo.TableProperty("urm:my_namespace:spark:table:schema:version", "0.0.1")
    ]

    def after_initialise(self, _result):
        self.create_as_unmanaged_delta_table()


# Table showing the use of a predefined property.  Note the need to provide the namespace to allow
# the URN to be in the form of urn:<namespace>:<specific-part>
class MyHiveTable3(repo.HiveRepo):
    table_name = "my_hive_table_3"

    table_properties = [
        repo.TableProperty(repo.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace")
    ]

    def after_initialise(self, _result):
        self.create_as_unmanaged_delta_table()
```

There are a number of defined URNs that define specific data agreement properties. They are available in the
ENUM `repo.DataAgreementType`:

+ `SCHEMA`.
    + URN is `urn:{ns}:spark:table:schema`
+ `SCHEMA_VERSION`.
    + URN is `urn:{ns}:spark:table:schema:version`
+ `PARTITION_COLUMNS`.
    + URN is `urn:{ns}:spark:table:schema:partitionColumns`.
    + Value is a comma separated list.
+ `PRUNE_COLUMN`.
    + URN is `urn:{ns}:spark:table:schema:pruneColumn`
+ `DATA_PRODUCT`.
    + URN is `urn:{ns}:dataProduct`
+ `PORT`.
    + URN is `urn:{ns}:dataProduct:port`
+ `UPDATE_FREQUENCY`.
    + URN is `urn:{ns}:dq:updateFrequency`
+ `CATALOGUE`.
    + URN is `urn:{ns}:catalogue`
+ `DESCRIPTION`.
    + URN is `urn:{ns}:catalogue:description`

#### Callbacks

`HiveTable` has a number of callback events which the table class can implement:

+ `after_initialise`. Called after the `HiveTable` `__init__` function has completed.
+ `after_append`. Called after the `write_append` function has completed.
+ `after_upsert`. Called after `upsert` has completed.
+ `after_stream_write_via_delta_upsert`. Called after `stream_write_via_delta_upsert`

One use of the callbacks is to create the table as a Hive table, or to update table properties.  `HiveTable` provides a
function called `create_as_unmanaged_delta_table`. This function creates an unmanaged delta table based on a schema
provided by table class. the `after_initialise` callback can be used to ensure the table is created with the appropriate
schema and properties before data is written to it.

```python
db = repo.Db(session=spark_test_session.create_session(), job_config=job_cfg())


class MyHiveTable(repo.HiveRepo):
    table_name = "my_hive_table"

    table_properties = [
        repo.TableProperty(repo.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace")
    ]

    def after_initialise(self):
        self.create_as_unmanaged_delta_table()

    def schema_as_dict(self):
        return {'fields': [
            {'metadata': {}, 'name': 'id', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'name', 'nullable': True, 'type': 'string'},
            {'metadata': {}, 'name': 'pythons', 'nullable': True, 'type': {
                'containsNull': True,
                'elementType': {'fields': [
                    {'metadata': {},
                     'name': 'id',
                     'nullable': True,
                     'type': 'string'}],
                    'type': 'struct'},
                'type': 'array'}},
            {'metadata': {}, 'name': 'season', 'nullable': True, 'type': 'string'}], 'type': 'struct'}


my_table = MyHiveTable(
    db=db)  # Executes the after_initialise callback with invokes the create_as_unmanaged_delta_table fn  
```

#### Write Functions

A `HiveTable` supports the following write functions:
+ `try_upsert`.  An `upsert` wrapped in a monadic try.  Returns a Option monad.
+ `upsert`. Performs a delta table merge operation.
+ `try_write_append`. A `write_append` wrapped in a monadic try.  Returns a Option monad.
+ `write_append`.  Appends rows to an existing table, or creates a new table from the dataframe if the table doesn't exist.
+ `try_write_stream`
+ `write_stream_append`
+ `try_stream_write_via_delta_upsert`
+ `stream_write_via_delta_upsert`


#### Write Options

All the write functions have the ability to add spark-type options.  Currently, the only option supported is `mergeSchema`.  Use the `Option` class to specify option requirements.

Options:
+ `Option.MERGE_SCHEMA`.  Write option `('mergeSchema', 'true')`, Spark config setter `('spark.databricks.delta.schema.autoMerge.enabled', 'true')`.  This option is used when the input dataframe has a different schema to the table being written to.  This can occur when the table is created prior to the first dataframe being written, or when a new dataframe requiring schema evolution is written.  Note that the schema evolution process for streams (especially delta streams) is a little different from batch appends and upserts.  The streaming version has performed by setting a configuration on the spark session, while the batch version uses Spark write options.   

```python
append_opts = [repo.Option.MERGE_SCHEMA]

# append a dataframe and merge the schema if necessary.
my_table.write_append(df, append_opts)
```

### Util Module

### Spark Session

### Secrets

The Secrets module obtains secrets using the Databricks DBUtils secrets utility. The module acts as a wrapper for
DButils. This allows for secrets to be mocked in tests without needing DBUtils. The CosmosDB repository is injected with
the secrets provider to enable secured access to CosmosDB.

The provider requires access to the Spark session when running on Databricks. However this is not required in test. You
also provide Secrets with a wrapper for DBUtils with also, optionally, takes a session. Both test and production
wrappers are available in the `util.databricks` module.

```python
from jobsworthy.util import secrets, databricks

provider = secrets.Secrets(session=di_container.session,
                           config=job_config(),
                           secrets_provider=databricks.DatabricksUtilsWrapper())
```

The default secret scope name is defined from the `JobConfig` properties; `domain_name` and `data_product_name`,
separated by a `.`. This can be overridden by defining the scope on the `Secrets` constructor, or on the call
to `get_secret`. It looks like this on the constructor.

```python
provider = secrets.Secrets(session=di_container.session,
                           config=job_config(),
                           secrets_provider=databricks.DatabricksUtilsWrapper(),
                           default_scope_name="custom-scope-name")
```

Getting a secret.

```python
provider.get_secret(secret_name="name-of-secret")  # returns an Either[secret-key]
```

Secrets is also able to return a `ClientCredential` using an Azure AD client credentials grant. The grant requires that
client id and secrets are obtainable via DBUtils through key-vault with the key names defined in `JobConfig` in the
properties `client_id_key` and `client_secret_key`

```python
provider.client_credential_grant()  # returns an Either[ClientCredential]
```

Testing using secrets. DBUtils is not available as an open source project. When creating the secrets provider, you can
provide a DBUtils mock class which is available. On this class you can also construct valid keys to be used for test (if
required; the mock returns a dummy key response to any generic lookup).

The example below also shows how to use a non-default scope on the `get_secrets` function.

```python
from jobsworthy.util import secrets, databricks

test_secrets = {"my_domain.my_data_product_name": {'my_secret': 'a-secret'},
                "alt_scope": {'my_secret': 'b-secret'}}

provider = secrets.Secrets(
    session=di_container.session,
    config=job_config(),
    secrets_provider=databricks.DatabricksUtilMockWrapper(spark_test_session.MockPySparkSession, test_secrets))

provider.get_secret(non_default_scope_name="alt_scope", secret_name="my_secret")
```

## Structure Model

The Structure module provides functions for building a more abstract definition of a Hive table schema and abstractions
for creating table, column and cell data which can be provided as the data argument when creating a dataframe.

### Table Schema

There are a number of ways to create a Hive table schema:

+ Create a dataframe from data where the schema can be inferred (e.g. a json or csv file), and write it to Hive.
+ Explicitly provide a schema on a create dataframe.
+ Create a Hive table using `pyspark.sql` `CREATE TABLE`, providing a schema.

The `Table` and `Column` classes abstract this schema creation. Creating the `StructType([])` for the table. In the end
the columns will come from `pyspark.sql.types`; for instance `StringType()`, `LongType()`, `ArrayType()`, `StructType()`
, etc.

There are 2 modes for creating table schema; DSL or construction functions. Both require the use of a vocabulary mapping
dictionary, which enables a domain-like term to be mapped to the column (or struct) term. A simple vocab might look like
this:

```python
def vocab():
    return {
        "columns": {
            "column1": {
                "hasDataProductTerm": "column_one"
            },
            "column2": {
                "hasDataProductTerm": "column_two",
                "sub1": {
                    "term": "sub_one"
                },
                "sub2": {
                    "term": "sub_two"
                }
            }
        }
    }
```

The structure of the vocab does not need to match the structure of the hive table column structure, but it can help with
visual mapping. In the vocab dict term mapping comes from using the dict key `hasDataProductTerm` or just `term`.

An example use of the vocab looks like this.

```python
from jobsworthy import structure as S

# initialise a Table with the vocab
table = S.Table(vocab=vocab())

# the vocab term "columns.column1", will result in a column named "column_one" 
table.column_factory(vocab_term="columns.column1")
```

Note that a table definition is created using the `Table` class. The structure of the abstract table is based on this
object.

#### Schema DSL

The DSL enables creating a table schema definition in a more fluid way, and inline.

The DSL provides the following commands:

+ `column()`. initiates the creation of a new column.
+ `struct(term, nullable)`. creates a column which will be a `StructType`. It is ended with the `end_struct`
  command.  `struct` and `end_struct` can be nested.
+ `string(term, nullable)`. creates a column of `StringType`, or defines a string within a struct.
+ `decimal(term, decimal_type, nullable)`. creates a column of `DecimalType`, or defines a decimal within a struct.
+ `long(term, nullable)`. creates a column of `LongType`, or defines a string within a struct.
+ `array(term, scalar_type, nullable)`. creates an array column of of a basic type (such as a `StringType()`), or
  defines an array within a struct.
+ `array_struct(term, nullable)`. creates a column of `ArrayType` which contains a struct, or defines a struct array
  within a struct.
+ `end_struct`. signal to declare that the definition of a struct is completed.

Here is an example uses the complete set of commands.

```python
from pyspark.sql.types import DecimalType, StringType
from jobsworthy import structure as S

table = (S.Table(vocab=vocab())
         .column()  # column1: string
         .string("columns.column1", nullable=False)

         .column()  # column 2 struct with strings
         .struct("columns.column2", nullable=False)
         .string("columns.column2.sub1", nullable=False)
         .string("columns.column2.sub1", nullable=False)
         .end_struct()

         .column()  # column 3: decimal
         .decimal("columns.column3", DecimalType(6, 3), nullable=False)

         .column()  # column 4: long
         .long("columns.column5", nullable=False)

         .column()  # column 5: array of strings
         .array("columns.column4", StringType, nullable=False)

         .column()  # column 6: array of structs
         .array_struct("columns.column6", nullable=True)
         .long("columns.column6.sub1", nullable=False)
         .string("columns.column6.sub1", nullable=False)
         .array("columns.column6.sub3", StringType, nullable=False)
         .end_struct()  # end column6

         .column()  # struct with strings and array of structs
         .struct("columns.column7", nullable=False)
         .string("columns.column7.sub1")

         .struct("columns.column7.sub2", nullable=False)  # struct nested in a struct
         .string("columns.column7.sub2.sub2-1")
         .string("columns.column7.sub2.sub2-2")
         .end_struct()

         .end_struct()  # end column7
         )
```

Note that a similar chained struct creation of a schema can also be achieved through the pyspark.sql.types module using
the `add()` function as the following example shows.

```python
from pyspark.sql import types as T

(T.StructType()
 .add("column_one", T.StringType(), False)
 .add('column_two', T.StructType().add("sub_two_one", T.StringType())))
```

Which is the same as:

```python
from pyspark.sql import types as T

(T.StructType([
    T.StructField("column_one", T.StringType(), False),
    T.StructField("column_one", T.StructType().add("sub_two_one", T.StringType()))
]))
```

