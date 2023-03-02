# Repository Module

The repo library offers a number of simple abstractions for managing Databrick/Spark databases and tables. It is by no
means an object-mapper. Rather its a few classes with some simple functions we have found useful when working with Hive
tables.

```python
from jobsworthy import repo
```

## SparkDB

`Db` is the base class representing a Hive Database. Once constructed it is provided to the hive table classes when they
are constructed.  There is no need to specialise the `repo.Db` class.  Simply instantiate it.

`Db` takes a [spark session](#spark-session) and a [job config](#job-configuration).

```python
db = repo.Db(session=spark_test_session.create_session(), config=job_config())
```

When initialised it checks that the database (defined in the config) exists and creates it if it doesn't.

The `repo.Db` class supports Hive DB properties.  To use these, specialise the `repo.Db` class, like so:

```python
class MyDb(repo.Db):
    db_properties = [
        repo.DbProperty(repo.DataAgreementType.DATA_PRODUCT, "my_dp", "my_namespace"),
        repo.DbProperty(repo.DataAgreementType.DESCRIPTION,
                        "A description of MyDb.",
                        "my_namespace"),
    ]

db = MyDb(session=spark_test_session.create_session(), config=job_config())
```

## Hive Table

The `HiveTable` class is an abstraction for a delta or hive table. Inheriting from this class provides a number of
helper and table management. It also provides common reading and writing functions (both stream and batch).

### Basic Configuration

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

### Table Lifecycle Events

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

### Table Schema

### Partitioning and Pruning

### Table Properties

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
properties are merged to the table.

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

### Callbacks

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

### Write Functions

A `HiveTable` supports the following write functions:
+ `try_upsert`.  An `upsert` wrapped in a monadic try.  Returns a Option monad.
+ `upsert`. Performs a delta table merge operation.
+ `try_write_append`. A `write_append` wrapped in a monadic try.  Returns a Option monad.
+ `write_append`.  Appends rows to an existing table, or creates a new table from the dataframe if the table doesn't exist.
+ `try_write_stream`
+ `write_stream_append`
+ `try_stream_write_via_delta_upsert`
+ `stream_write_via_delta_upsert`


### Write Options

All the write functions have the ability to add spark-type options.  Currently, the only option supported is `mergeSchema`.  Use the `Option` class to specify option requirements.

Options:
+ `Option.MERGE_SCHEMA`.  Write option `('mergeSchema', 'true')`, Spark config setter `('spark.databricks.delta.schema.autoMerge.enabled', 'true')`.  This option is used when the input dataframe has a different schema to the table being written to.  This can occur when the table is created prior to the first dataframe being written, or when a new dataframe requiring schema evolution is written.  Note that the schema evolution process for streams (especially delta streams) is a little different from batch appends and upserts.  The streaming version has performed by setting a configuration on the spark session, while the batch version uses Spark write options.   

```python
append_opts = [repo.Option.MERGE_SCHEMA]

# append a dataframe and merge the schema if necessary.
my_table.write_append(df, append_opts)
```
