# Jobsworth

A set of utility functions and classes to aid in build Spark jobs on Azure databricks.

## Job Configuration

## Spark Job Module

Job provides a decorator which wraps the execution of a spark job.  You use the decorator at the entry point for the job.
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

The initialisers must be imported before the job function is called; to ensure they are registered.  To do that, either import them directly in the job module, or add them to a module `__init__.py` and import the module.


## Model Library

### Streamer

The `Streamer` module provides a fluent streaming abstraction on top of 2 hive repos (the from and to repos).  The Streamer runs a pipeline as follows:

+ Read from the source repo.
+ Perform a transformation.
+ Write to the target repo.  This is where the stream starts and uses pyspark streaming to perform the read, transform and write.
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

Some transformation functions require data from outside the input table.  You can configure the streamer with additional transformation context by passing in kwargs on the `with_transformer`.

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

When configuring the `stream_to` table, you can provide partition columns when writing the stream.  Provide a tuple of column names. 

```python
streamer = model.STREAMER().stream_to(to_table, ('name', ))
```

### Repository Module

The repo library offers a number of simple abstractions for managing Databrick/Spark databases and tables.  It is by no means an object-mapper.  Rather its a few classes with some simple functions we have found useful when working with Hive tables.

```python
from jobsworthy import repo
```

### SparkDB

`Db` is the base class representing a Hive Database.  Once constructed it is provided to the hive table classes when they are constructed.

`Db` takes a [spark session](#spark-session) and a [job config](#job-configuration).

```python
db = repo.Db(session=spark_test_session.create_session(), config=job_config())
```

When intialised it checks that the database (defined in the config) exists and creates it if it doesn't.

### Hive Table

The `HiveTable` class is an abstraction for a delta or hive table.  Inheriting from this class provides a number of helper and table management.  It also provides common reading and writing functions (both stream and batch).

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

#### Table Schema

#### Partitioning and Pruning

#### Table Properties

Hive table properties provide a means of storing key value pairs in the table, which are retrievable as a dataframe.  The table property key is a URN, while the value is a string.  To store more complex objects in the value, requires serialisation into a string and interpretation outside the library.

Use `repo.TableProperty` class to declare properties.  This class takes a number of key formats; a URN, URN without the URN portion (as a shortcut) and a common table property using the repo.DataAgreementType enum.

The table properties are declared as a class property on the table.  The repo module is then able to maintain those properties on the table.  The merging of properties is the responsibility of the table instance.  Calling `self.property_manager.merge_table_properties()` will explicitly merge the declared difference of the properties defined in the table with the properties on the Hive Table itself.

When table properties are declared, and the Hive table is created (`create_as_unmanaged_delta_table()`) the table proeprties are merged to the table.

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

There are a number of defined URNs that define specific data agreement properties.  They are available in the ENUM `repo.DataAgreementType`: 

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
+ `after_initialise`.  Called after the `HiveTable` `__init__` function has completed.
+ `after_append`.  Called after the `write_append` function has completed.

One use of the callbacks is to create the table as a Hive table, or to update table properties.  `HiveTable` provides a function called `create_as_unmanaged_delta_table`.  This function creates an unmanaged delta table based on a schema provided by table class.  the `after_initialise` callback can be used to ensure the table is created with the appropriate schema and properties before data is written to it.

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


my_table = MyHiveTable(db=db)  # Executes the after_initialise callback with invokes the create_as_unmanaged_delta_table fn  
```





## Util Module

### Spark Session

### Secrets

The Secrets module obtains secrets using the Databricks DBUtils secrets utility.  The module acts as a wrapper for DButils.  This allows for secrets to be mocked in tests without needing DBUtils.  The CosmosDB repository is injected with the secrets provider to enable secured access to CosmosDB.

The provider requires access to the Spark session when running on Databricks.  However this is not required in test.  You also provide Secrets with a wrapper for DBUtils with also, optionally, takes a session.  Both test and production wrappers are available in the `util.databricks` module.

```python
from jobsworthy.util import secrets, databricks

provider = secrets.Secrets(session=di_container.session,
                           config=job_config(),
                           secrets_provider=databricks.DatabricksUtilsWrapper())
```

The default secret scope name is defined from the `JobConfig` properties; `domain_name` and `data_product_name`, separated by a `.`.  This can be overridden by defining the scope on the `Secrets` constructor, or on the call to `get_secret`.  It looks like this on the constructor.

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

Secrets is also able to return a `ClientCredential` using an Azure AD client credentials grant.  The grant requires that client id and secrets are obtainable via DBUtils through key-vault with the key names defined in `JobConfig` in the properties `client_id_key` and `client_secret_key` 

```python
provider.client_credential_grant()   # returns an Either[ClientCredential]
```

Testing using secrets.  DBUtils is not available as an open source project.  When creating the secrets provider, you can provide a DBUtils mock class which is available.  On this class you can also construct valid keys to be used for test (if required; the mock returns a dummy key response to any generic lookup).

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