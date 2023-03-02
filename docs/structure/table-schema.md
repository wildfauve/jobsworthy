# Table Schema

There are a number of ways to create a Hive table schema:

+ Create a dataframe from data where the schema can be inferred (e.g. a json or csv file), and write it to Hive.
+ Explicitly provide a schema on a create dataframe.
+ Create a Hive table using `pyspark.sql` `CREATE TABLE`, providing a schema.

The `Table` and `Column` classes abstract this schema creation. Creating the `StructType([])` for the table. In the end
the columns will come from `pyspark.sql.types`; for instance `StringType()`, `LongType()`, `ArrayType()`, `StructType()`
, etc.

There are 2 modes for creating table schema; DSL or construction functions. Both require the use of a vocabulary mapping
dictionary.


## The Vocab
The Vocab enables a domain-like term to be mapped to the column (or struct) term. A simple vocab might look like
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

## Schema DSL

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
