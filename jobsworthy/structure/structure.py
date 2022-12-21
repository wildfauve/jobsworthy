from typing import Any, Callable, Dict, List, Tuple, Union
import json
from functools import reduce
from pymonad.tools import curry
from pyspark.sql.types import StructType

from jobsworthy.util import fn, json_util, monad
from jobsworthy.util import error
from . import schema_util as su, vocab_util as V, value


def default_cell_builder(cell):
    return cell.props


def always_valid_validator(cell):
    return monad.Right(None)


def default_exception_struct_fn(term, vocab):
    return su.build_string_field(term, vocab, nullable=True)


class RootParent:
    column_name = "__RootMeta__"

    def __init__(self, meta: str, tracer: Any):
        self.meta = meta
        self.tracer = tracer

    def meta_props(self):
        return {'columnName': self.column_name, 'meta': self.meta, 'lineage_id': self.tracer.trace}

    def identity_props(self):
        return {self.column_name: {'identity': self.meta, 'lineage_id': self.tracer.trace}}


class Vocab:
    def __init__(self, vocab):
        self.vocab = vocab


class Struct:
    def __init__(self, callback, vocab: Dict):
        self.callback = callback
        self.vocab = vocab
        self.schema = None
        self.fields = []

    def string(self,
               term,
               nullable: bool = False):
        self.fields.append(su.build_string_field(term, self.vocab, nullable=nullable))
        return self

    def decimal(self,
                term,
                decimal_type,
                nullable: bool = False):
        self.fields.append(su.build_decimal_field(term, self.vocab, decimal_type, nullable=nullable))
        return self

    def long(self,
             term,
             nullable: bool = False):
        self.fields.append(su.build_long_field(term, self.vocab, nullable=nullable))
        return self

    def array(self,
              term,
              scalar_type,
              nullable: bool = False):
        self.fields.append(su.build_array_field(term, self.vocab, scalar_type(), nullable=nullable))
        return self


    def struct(self,
               term,
               nullable: bool = False):
        self.term = term
        self.nullable = nullable
        return Struct(self.nested_struct_callback, self.vocab)

    def array_struct(self,
                     term,
                     nullable: bool = False):
        self.term = term
        self.nullable = nullable
        return Struct(self.end_array_struct, self.vocab)

    def end_array_struct(self, struct_type):
        self.fields.append(su.build_array_field(self.term, self.vocab, struct_type, self.nullable))
        return self


    def nested_struct_callback(self, struct_type):
        self.fields.append(su.build_struct_field(self.term, self.vocab, struct_type, self.nullable))
        return self

    def end_struct(self):
        return self.callback(StructType(self.fields))




class Column:

    def __init__(self,
                 vocab: Dict,
                 vocab_term: str = None,
                 struct_fn: Callable = None,
                 callback: Callable = None,
                 validator: Callable = always_valid_validator,
                 cell_builder: Callable = default_cell_builder):
        self.vocab_term = vocab_term
        self.vocab = vocab
        self.struct_fn = struct_fn
        self.validator = validator
        self.cell_builder = cell_builder
        self.schema = None
        self.callback = callback
        if self.struct_fn:
            self.build_dataframe_struct_schema()

    def build_dataframe_struct_schema(self):
        self.schema = self.struct_fn(self.vocab_term, self.vocab)

    def __eq__(self, other):
        return self.schema.name == other.schema.name

    def schema_name(self):
        return self.schema.name

    def generate_exception_column(self):
        return self.__class__(vocab_term=self.vocab_term,
                              vocab=self.vocab,
                              struct_fn=default_exception_struct_fn)

    def string(self,
               term,
               nullable: bool = False,
               validator: Callable = always_valid_validator,
               cell_builder: Callable = default_cell_builder):
        self.schema = su.build_string_field(term, self.vocab, nullable=nullable)
        return self.callback

    def decimal(self,
                term,
                decimal_type,
                nullable: bool = False,
                validator: Callable = always_valid_validator,
                cell_builder: Callable = default_cell_builder):
        self.schema = su.build_decimal_field(term, self.vocab, decimal_type, nullable=nullable)
        return self.callback

    def long(self,
             term,
             nullable: bool = False,
             validator: Callable = always_valid_validator,
             cell_builder: Callable = default_cell_builder):
        self.schema = su.build_long_field(term, self.vocab, nullable=nullable)
        return self.callback

    def array(self,
              term,
              scalar_type,
              nullable: bool = False,
              validator: Callable = always_valid_validator,
              cell_builder: Callable = default_cell_builder):
        self.schema = su.build_array_field(term, self.vocab, scalar_type(), nullable=nullable)
        return self.callback

    def struct(self,
               term,
               nullable: bool = False,
               validator: Callable = always_valid_validator,
               cell_builder: Callable = default_cell_builder):
        self.term = term
        self.nullable = nullable
        return Struct(self.end_struct, self.vocab)

    def end_struct(self, struct_type):
        self.schema = su.build_struct_field(self.term, self.vocab, struct_type, self.nullable)
        return self.callback

    def array_struct(self,
                     term,
                     nullable: bool = False,
                     validator: Callable = always_valid_validator,
                     cell_builder: Callable = default_cell_builder):
        self.term = term
        self.nullable = nullable
        return Struct(self.end_array_struct, self.vocab)

    def end_array_struct(self, struct_type):
        self.schema = su.build_array_field(self.term, self.vocab, struct_type, self.nullable)
        return self.callback



class Table:

    def __init__(self,
                 columns: List[Column] = None,
                 vocab: Dict = None,
                 vocab_directives: List[value.VocabDirective] = None):
        self.columns = columns if columns else []
        if vocab_directives:
            self.vocab = (vocab_directives, vocab if vocab else {})
        else:
            self.vocab = vocab if vocab else {}

    def column_factory(self,
                       vocab_term: str,
                       struct_fn: Callable,
                       cell_builder: Callable = None,
                       validator: Callable = None):
        col = Column(vocab_term=vocab_term,
                     vocab=self.vocab,
                     struct_fn=struct_fn,
                     cell_builder=cell_builder,
                     validator=validator)
        self.columns.append(col)
        return col

    def column(self):
        """
        Entrypoint for the DSL version of the schema builder.
        :return:
        """
        col = Column(callback=self, vocab=self.vocab)
        self.columns.append(col)
        return col

    def hive_schema(self):
        return StructType(list(map(lambda column: column.schema, self.columns)))

    def exception_table(self):
        return self.__class__(vocab=self.vocab,
                              columns=list(map(lambda column: column.generate_exception_column(), self.columns)))

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
        self._validations_results = self.validate()
        return self._validations_results

    def validate(self):
        results = self.column.validator(self) if self.column.validator else monad.Right(None)
        if not hasattr(results, 'lift') or not isinstance(results.lift(), dict):
            return monad.Right(None)
        return results

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

    def cell_factory(self, column_or_vocab: Union[Column, str]) -> Cell:
        if not isinstance(column_or_vocab, str):
            return self.cell_from_column(column_or_vocab)
        term, _meta = V.term_and_meta(column_or_vocab, self.table.vocab)
        return self.cell_from_schema_name(term)

    def cell_from_schema_name(self, name: str):
        column = fn.find(self.schema_name_predicate(name), self.table.columns)
        if not column:
            raise error.SchemaMatchingError(f"Can not find column with term {name}")
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
