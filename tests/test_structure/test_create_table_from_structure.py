from jobsworthy import structure as S
from jobsworthy import repo


def test_create_file_from_dsl_structure(test_db):
    table1 = Table1(db=test_db)

    df = table1.read()

    # TODO: The create table col spec doesnt current support non-null sub columns.

    assert df.schema == table1_definition().hive_schema()


# Helpers

def table1_definition():
    return (S.Table(vocab=vocab())
            .column()  # column1: string
            .string("columns.column1", nullable=False)

            .column()  # column 2 struct with strings
            .struct("columns.column2", nullable=False)
            .string("columns.column2.sub1", nullable=True)   # these have to be True as False not supported.
            .string("columns.column2.sub2", nullable=True)
            .array_struct("columns.column2.sub3", True)
            .string("columns.column2.sub3.sub3-1", True)
            .string("columns.column2.sub3.sub3-2", True)
            .end_struct()
            .end_struct())


def vocab():
    return {
        "columns": {
            "column1": {
                "term": "column_one"
            },
            "column2": {
                "term": "column_two",
                "sub1": {
                    "term": "sub_one"
                },
                "sub2": {
                    "term": "sub_two"
                },
                "sub3": {
                    "term": "sub_three",
                    "sub3-1": {
                        "term": "sub_three_one"
                    },
                    "sub3-2": {
                        "term": "sub_three_two"
                    }
                }
            }
        }
    }

class Table1(repo.HiveRepo):
    table_name = "table1"

    schema = table1_definition().hive_schema()

    def after_initialise(self):
        self.create_as_managed_delta_table()