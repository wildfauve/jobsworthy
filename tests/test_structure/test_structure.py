import pytest
import json

from jobsworthy.util import monad, error
from jobsworthy import structure as S


def test_cell_name_from_vocab():
    cell = S.Cell(column=column1(),
                  identity='id',
                  props={'at_id': 1, 'at_type': "t", 'label': "one"})

    assert cell.column_name() == "column_one"


def test_cell_build():
    cell = S.Cell(column=column1(),
                  identity='id',
                  props={'at_id': 1, 'at_type': "t", 'label': "one"})

    assert cell.build() == (1, 't', 'one')


def test_build_a_table_using_the_factory():
    table = table_using_factory()

    assert table.columns[0].schema_name() == "column_one"


def test_build_a_table_using_injected_columns():
    table = table_with_injected_columns()

    assert table.columns[0].schema_name() == "column_one"


def test_has_parent():
    root_parent = S.RootParent(meta="some-meta", tracer=generate_root_parent())
    cell1 = S.Cell(column=column1(),
                   identity='id1',
                   props={'at_id': 1, 'at_type': "t", 'label': "one"}).has_parent(root_parent)

    cell2 = S.Cell(column=column1(),
                   identity='id2',
                   props={'at_id': 2, 'at_type': "t", 'label': "two"}).has_parent(cell1)

    assert cell2.parent.meta_props() == cell1.meta_props()


def test_cell_recursively_returns_parent_meta():
    root_parent = S.RootParent(meta="some-meta", tracer=generate_root_parent())
    cell1 = S.Cell(column=column1(),
                   identity='id1',
                   props={'at_id': 1, 'at_type': "t", 'label': "one"}).has_parent(root_parent)

    cell2 = S.Cell(column=column1(),
                   identity='id2',
                   props={'at_id': 2, 'at_type': "t", 'label': "two"}).has_parent(cell1)

    expected_meta = {'column_one': {'identity': 'id1'}, '__RootMeta__': {'identity': 'some-meta', 'lineage_id': 't1'}}

    assert cell2.parents_meta_props() == expected_meta


def test_cell_with_no_parent_returns_empty_meta():
    cell1 = S.Cell(column=column1(),
                   identity='id',
                   props={'at_id': 1, 'at_type': "t", 'label': "one"})
    assert not cell1.parent
    assert cell1.parents_meta_props() == {}


def test_table_hive_schema_builder():
    table = table_using_factory()

    expected_schema_names = ['column_one', 'column_two']

    assert [col.name for col in table.hive_schema()] == expected_schema_names


def test_column_builds_schema():
    expected_schema = {'metadata': {},
                       'name': 'column_one',
                       'nullable': False,
                       'type': {
                           'fields': [
                               {'metadata': {},
                                'name': 'id',
                                'nullable': False,
                                'type': 'string'},
                               {'metadata': {},
                                'name': 'type',
                                'nullable': False,
                                'type': 'string'},
                               {'metadata': {'term': 'lcc-lr:hasTag'},
                                'name': 'label',
                                'nullable': True,
                                'type': 'string'}],
                           'type': 'struct'}}

    assert column1().schema.jsonValue()


def test_column_generates_exception_column():
    exception_column = column1().generate_exception_column()

    expected_schema = {'metadata': {}, 'name': 'column_one', 'nullable': True, 'type': 'string'}

    assert json.loads(exception_column.schema.json()) == expected_schema


def test_generates_exception_schema():
    schema = table_using_factory().exception_table().hive_schema()

    expected_columns = ['column_one', 'column_two']

    assert [col.name for col in schema] == expected_columns


def test_row_generates_cell_by_term_and_column():
    table = table_using_factory()

    row1 = table.row_factory()

    cell1 = row1.cell_factory("column_one")

    assert cell1.column.vocab_term == 'columns.column1'


def test_row_generates_cell_by_column_object():
    table = table_with_injected_columns()

    row1 = table.row_factory()

    column1 = table.columns[0]

    cell1 = row1.cell_factory(column1)

    assert cell1.column.vocab_term == 'columns.column1'


def test_builds_a_row():
    table = table_using_factory()

    row1 = table.row_factory()
    row2 = table.row_factory()

    row1_cell1 = row1.cell_factory("column_one").values(identity="id1-1",
                                                        props={'at_id': 10, 'at_type': "t", 'label': "r1c1"})
    row1_cell2 = row1.cell_factory("column_two").values(identity="id2-1",
                                                        props={'at_id': 20, 'at_type': "t", 'label': "r1c2"})

    row2_cell1 = row2.cell_factory("column_one").values(identity="id1-2",
                                                        props={'at_id': 11, 'at_type': "t", 'label': "r2c1"})
    row2_cell2 = row2.cell_factory("column_two").values(identity="id2-2",
                                                        props={'at_id': 21, 'at_type': "t", 'label': "r2c2"})

    assert row1.build_ordered_row_values() == ((10, 't', 'r1c1'), (20, 't', 'r1c2'))
    assert row2.build_ordered_row_values() == ((11, 't', 'r2c1'), (21, 't', 'r2c2'))


def test_builds_a_row_as_exception_when_no_exception():
    table = table_using_factory()

    row1 = table.row_factory()

    row1_cell1 = row1.cell_factory("column_one").values(identity="id1-1",
                                                        props={'at_id': 10, 'at_type': "t", 'label': "r1c1"})
    row1_cell2 = row1.cell_factory("column_two").values(identity="id2-1",
                                                        props={'at_id': 20, 'at_type': "t", 'label': "r1c2"})

    expected_row = ('{"column_one": {"at_id": 10, "at_type": "t", "label": "r1c1"}, "validationErrors": null}',
                    '{"column_two": {"at_id": 20, "at_type": "t", "label": "r1c2"}, "validationErrors": null}')

    assert row1.build_ordered_row_values_as_exception() == expected_row


def test_builds_a_row_as_exception():
    table = table_with_errors()

    row1 = table.row_factory()

    row1_cell1 = row1.cell_factory("column_one").values(identity="id1-1",
                                                        props={'at_id': 10, 'at_type': "t", 'label': "r1c1"})

    expected_row = (
        '{"column_one": {"at_id": 10, "at_type": "t", "label": "r1c1"}, "validationErrors": {"at_id": ["null value not allowed"], "at_type": ["required field"], "label": ["null value not allowed"]}}',)

    assert row1.build_ordered_row_values_as_exception() == expected_row


def test_raises_exception_when_column_cant_be_found():
    table = table_using_factory()
    row1 = table.row_factory()

    with pytest.raises(error.SchemaMatchingError):
        row1.cell_factory('invalid_column_name')


#
# Helpers
#

def generate_root_parent():
    class RunProxy:
        trace = "t1"
        pass

    return RunProxy()


def table_using_factory():
    table = S.Table(vocab=vocab())
    table.column_factory(vocab_term="columns.column1",
                         struct_fn=id_type_label_struct,
                         validator=success_validator,
                         cell_builder=id_type_label_builder)

    table.column_factory(vocab_term="columns.column2",
                         struct_fn=id_type_label_struct,
                         validator=success_validator,
                         cell_builder=id_type_label_builder)

    return table


def table_with_errors():
    table = S.Table(vocab=vocab())
    table.column_factory(vocab_term="columns.column1",
                         struct_fn=id_type_label_struct,
                         validator=failure_validator,
                         cell_builder=id_type_label_builder)

    return table


def table_with_injected_columns():
    return S.Table(vocab=vocab(), columns=[column1()])


def vocab():
    return {
        "columns": {
            "column1": {
                "hasDataProductTerm": "column_one"
            },
            "column2": {
                "hasDataProductTerm": "column_two"
            }
        }
    }


def column1():
    return S.Column(vocab_term="columns.column1",
                    vocab=vocab(),
                    struct_fn=id_type_label_struct,
                    validator=success_validator,
                    cell_builder=id_type_label_builder)


def column2():
    return S.Column(vocab_term="columns.column2",
                    vocab=vocab(),
                    struct_fn=id_type_label_struct,
                    validator=monad_success_validator,
                    cell_builder=id_type_label_builder)


def id_type_label_struct(term, vocab):
    return S.build_struct_field(term, vocab, S.type_id_label_struct, nullable=False)


def success_validator(_cell):
    return True


def monad_success_validator(_cell):
    return monad.Right(None)


def failure_validator(_cell):
    return monad.Left({'at_id': ['null value not allowed'],
                       'at_type': ['required field'],
                       'label': ['null value not allowed']})


def id_type_label_builder(cell):
    return (cell.props['at_id'], cell.props['at_type'], cell.props['label'])
