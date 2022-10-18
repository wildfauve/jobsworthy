import decimal
import json
from functools import reduce
from typing import Callable, Dict, List, Optional, Tuple, Union
from jobsworth.util import fn, json_util, monad

from jobsworth.util import error
from jobsworth.structure.util import schema_util as su
from pymonad.tools import curry
from pyspark.sql.types import StructType

from jobsworth.structure.util import vocab_util as V


def default_cell_builder(cell):
    return cell.props


def always_valid_validator(cell):
    return monad.Right(None)


def default_exception_struct_fn(vocab_term):
    return su.build_string_field(vocab_term, nullable=True)


class RootParent:
    column_name = "__RootMeta__"

    def __init__(self, meta: str, run):
        self.meta = meta
        self.run = run

    def meta_props(self):
        return {'columnName': self.column_name, 'meta': self.meta, 'lineage_id': self.run.trace}

    def identity_props(self):
        return {self.column_name: {'identity': self.meta, 'lineage_id': self.run.trace}}


class Column:

    def __init__(self,
                 vocab_term: str,
                 struct_fn: Callable,
                 validator: Callable = always_valid_validator,
                 cell_builder: Callable = default_cell_builder):
        self.vocab_term = vocab_term
        self.struct_fn = struct_fn
        self.validator = validator
        self.cell_builder = cell_builder
        self.build_dataframe_struct_schema()

    def build_dataframe_struct_schema(self):
        self.schema = self.struct_fn(self.vocab_term)

    def __eq__(self, other):
        return self.schema.name == other.schema.name

    def generate_exception_column(self):
        return self.__class__(vocab_term=self.vocab_term,
                              struct_fn=default_exception_struct_fn)


class Table:

    def __init__(self, columns: List[Column]):
        self.columns = columns

    def hive_schema(self):
        return StructType(list(map(lambda column: column.schema, self.columns)))

    def exception_table(self):
        return self.__class__(columns=list(map(lambda column: column.generate_exception_column(), self.columns)))

    def row_factory(self):
        return Row(self)


class Cell:
    def __init__(self, column: Column, props: Union[Dict, List, Tuple, str] = None, identity: str = None):
        self.column = column
        self.props = props
        self.identity = identity
        self.parent = None
        self._validations_results = None
        pass

    def values(self, props: Union[Dict, List, Tuple, str], identity: str = None):
        self.props = props
        self.identity = identity
        return self

    def validation_results(self):
        if self._validations_results:
            return self._validations_results
        self._validations_results = self.column.validator(self) if self.column.validator else monad.Right(None)
        return self._validations_results

    def has_parent(self, parent):
        self.parent = parent
        return self

    def build(self):
        return self.column.cell_builder(self)

    def to_dict(self):
        return self.props

    def as_cell_dict(self):
        return {self.column_name(): self.props}

    def cell_dict_with_errors(self):
        return {**self.as_cell_dict(), "validationErrors": self.validation_results().lift()}

    def root_parent(self):
        if not self.parent:
            return None
        if isinstance(self.parent, RootParent):
            return self.parent
        return self.parent.root_parent()

    def meta_props(self):
        return {'columnName': self.column_name(), 'identity': self.identity}

    def identity_props(self):
        return {self.column_name(): {'identity': self.identity}}

    def column_name(self):
        return self.column.schema.name

    def parents_meta_props(self, meta_props: dict = {}):
        if not self.parent:
            return meta_props
        if isinstance(self.parent, RootParent):
            return {**meta_props, **self.parent.identity_props()}
        return self.parent.parents_meta_props({**meta_props, **self.parent.identity_props()})


class Row:
    def __init__(self, table: Table):
        self.table = table
        self.cells = []

    def cell_factory(self, column_vocab) -> Cell:
        term, _meta = V.term_and_meta(column_vocab)
        column = fn.find(self.schema_name_predicate(term), self.table.columns)
        if not column:
            raise error.SchemaMatchingError(f"Can not find column with term {term}")
        cell = Cell(column=column)
        self.cells.append(cell)
        return cell

    def cell_from_column(self, column: Column) -> Cell:
        if not isinstance(column, Column):
            raise error.SchemaMatchingError(f"Column {column.__class__.__name__} is not an instance of Column")
        cell = Cell(column=column)
        self.cells.append(cell)
        return cell

    def build_ordered_row_values(self):
        return reduce(self.build_cell, self.table.columns, tuple())

    def build_ordered_row_values_as_exception(self):
        return reduce(self.cell_exception_builder, self.table.columns, tuple())

    def cell_exception_builder(self, row_value, column):
        cell = fn.find(self.find_cell_predicate(column), self.cells)
        if not cell:
            return row_value + (None,)

        return row_value + (json.dumps(cell.cell_dict_with_errors(), cls=json_util.CustomLogEncoder),)

    def build_cell(self, row_value, column):
        cell = fn.find(self.find_cell_predicate(column), self.cells)
        if not cell:
            return row_value + (None,)

        return row_value + (cell.build(),)

    def all_cells_in_row_ok(self) -> bool:
        return all(map(monad.maybe_value_ok, [cell.validation_results() for cell in self.cells]))

    @curry(3)
    def find_cell_predicate(self, column, cell):
        return cell.column == column

    @curry(3)
    def schema_name_predicate(self, term, column):
        return column.schema.name == term


#
# Predicates
#
def all_cells_ok(row):
    return row.all_cells_in_row_ok()


def literal_string_builder(cell: Cell) -> str:
    return cell.props['stringLiteral']


def literal_time_builder(cell: Cell) -> str:
    return cell.props['timeLiteral']

def literal_date_builder(cell: Cell) -> str:
    return cell.props['dateLiteral']


def literal_decimal_builder(cell: Cell) -> str:
    return cell.props['decimalLiteral']
