from typing import Tuple, Any, Callable, List
import re
from functools import reduce, partial
from pathlib import Path
from rich.tree import Tree
from rich import print as rprint
from rich.table import Table

from jobsworthy.structure.puml.model import class_model
from jobsworthy.util import fn, error, console, dataklass

type_to_pyspark_types = {
    'string': 'StringType',
    'long': 'LongType',
    'integer': 'IntegerType',
    'decimal': 'DecimalType',
    'array': 'ArrayType',
    'struct': 'StructType'
}

primitive_types = ['string', 'long', 'integer', 'decimal']


def build(file_name: str, model_name: str):
    return _parse(_read_file(file_name).split("\n"),
                  class_model.Model(name=model_name),
                  None,
                  [],
                  0)


def _parse(lines,
           model: class_model.Model,
           this_klass: class_model.Klass,
           up_klass: List[class_model.Klass],
           column_ct):
    fst, rst = fn.fst_rst(lines)
    console.console.print(f"\n\n[yellow] {fst}")
    func, match = _parser_finder(fst)
    if not func:
        console.console.print(f"Parse No Func: {fst}")
        if not rst:
            return model
        return _parse(rst, model, this_klass, up_klass, column_ct)
    new_rst, new_this_klass, new_up_klass, new_ct = func(match, model, fst, rst, this_klass, up_klass, column_ct)
    if not rst:
        return model
    return _parse(new_rst, model, new_this_klass, new_up_klass, new_ct)


def _parser_finder(line: str):
    result = fn.remove_none(map(partial(_re_predicate, line), rx.items()))
    if not result:
        if line:
            console.console.print(f"No Match for: {line}", style="#af00ff")
        return None, None
    return result[0]


def _re_predicate(target: str, rx):
    name, options = rx
    if not target:
        return None
    match = options['re'].search(target)
    if not match:
        return None
    return (options['fn'], match)


def _model_and_class(match, model, _fst, rst, _this_klass, up_klass, column_ct):
    name, = match.groups()
    table = model.create_klass(name, "table")
    up_klass.append(table)
    return rst, table, up_klass, column_ct


def _column_class(match, model, fst, rst, this_klass, up_klass, column_ct):
    nesting, name, type_of, optional_type_extension, _, nullable = match.groups()
    if not _type_is_a_struct(type_of):
        return _scalar_column_class(match, model, fst, rst, this_klass, up_klass, column_ct)
    return _struct_column_class(match, model, fst, rst, this_klass, up_klass, column_ct)


def _struct_column_class(match, model, fst, rst, this_klass, up_klass, column_ct):
    nesting, name, type_of, optional_type_extension, _, nullable = match.groups()

    column = model.create_klass(name, "column").add_single_meta('isAtColumnPosition', column_ct)

    _add_klass_to_nesting_level(column, up_klass, nesting)

    _add_class_relationship(model, up_klass[_nested_count(nesting) - 1], "-->", None, column, None)

    if _primitive_array(type_of, rst):
        return _primitive_struct_array_property(match, model, fst, rst, column, up_klass, column_ct + 1)

    if _array_of_struct(type_of, rst):
        return _column_array_struct(rx['column_prop']['re'].search(rst[0]),
                                    model,
                                    rst[0],
                                    rst[1:],
                                    this_klass,
                                    up_klass,
                                    column_ct)

    console.console.print(
        f"[blue] _struct_column_class: Class: {column.name}({column.__class__.__name__}) ~{name}, {type_of}")
    return rst, column, up_klass, column_ct + 1


def _column_array_struct(match, model, fst, rst, this_klass, up_klass, column_ct):
    nesting, name, type_of, optional_type_extension, _, nullable = match.groups()

    struct = model.create_klass(f"{up_klass[_nested_count(nesting) - 1].name}ArrayStruct", "array_struct")

    _add_klass_to_nesting_level(struct, up_klass, nesting)

    _add_class_relationship(model, up_klass[_nested_count(nesting) - 1], "-->", "1..*", struct, None)

    console.console.print(
        f"[blue] _column_array_struct: Class: {struct.name}({struct.__class__.__name__}) ~{name}, {type_of}")

    return rst, struct, up_klass, column_ct + 1


def _struct_struct_class(match, model, fst, rst, this_klass, up_klass, column_ct):
    nesting, name, type_of, optional_type_extension, _, nullable = match.groups()

    struct = _find_or_create_struct(model, name)

    _add_klass_to_nesting_level(struct, up_klass, nesting)

    _add_class_relationship(model, up_klass[_nested_count(nesting) - 1], "-->", None, struct, None)

    console.console.print(
        f"[blue] _struct_struct_class: Class: {struct.name}({struct.__class__.__name__}) ~{name}, {type_of}")

    if _primitive_array(type_of, rst):
        return _primitive_struct_array_property(match, model, fst, rst, struct, up_klass, column_ct)

    if _array_of_struct(type_of, rst):
        console.console.print(f"[blue] _struct_struct_class: gobbled {rst[0]}")
        return rst[1:], struct, up_klass, column_ct

    return rst, struct, up_klass, column_ct


def _primitive_struct_array_property(match, model, fst, rst, this_klass, up_klass, column_ct):
    nesting, name, type_of, optional_type_extension, _, nullable = match.groups()
    nxt, rst = fn.fst_rst(rst)

    nesting, nxt_name, nxt_type_of, nxt_optional_type_extension, _, nxt_nullable = rx['column_prop']['re'].search(
        nxt).groups()

    this_klass.add_property(name, _build_type_definition(nxt_type_of,
                                                         nxt_optional_type_extension,
                                                         nxt_nullable,
                                                         "Array[{}]"))

    console.console.print(
        f"[blue] _primitive_struct_array_property: Class: {this_klass.name}({this_klass.__class__.__name__}) ~{name}, {nxt_type_of}")

    return rst, this_klass, up_klass, column_ct


def _scalar_column_class(match, model, _fst, rst, this_klass, up_klass, column_ct):
    nesting, name, type_of, optional_type_extension, _, nullable = match.groups()
    column = model.create_klass(name, "column")
    column.add_property(name, _build_type_definition(type_of, optional_type_extension, nullable))
    column.add_single_meta('isAtColumnPosition', column_ct)

    _add_class_relationship(model, up_klass[_nested_count(nesting) - 1], "-->", None, column, None)

    console.console.print(
        f"[blue] scalar_column_class: Class: {column.name}({column.__class__.__name__}) ~{name}, {type_of}")

    return rst, this_klass, up_klass, column_ct + 1


def _column_prop(match, model, fst, rst, this_klass, up_klass, column_ct):
    nesting, name, type_of, optional_type_extension, _, nullable = match.groups()

    if _primitive_type(type_of):
        return _primitive_property(match, model, fst, rst, this_klass, up_klass, column_ct)

    if _primitive_array(type_of, rst):
        return _primitive_struct_array_property(match, model, fst, rst, this_klass, up_klass, column_ct)

    if _type_is_a_struct(type_of):
        return _struct_struct_class(match, model, fst, rst, this_klass, up_klass, column_ct)
    raise error.TableSchemaException(f"Column Prop parser is unknown type from line {fst}")


def _primitive_property(match, model, fst, rst, this_klass, up_klass, column_ct):
    nesting, name, type_of, optional_type_extension, _, nullable = match.groups()

    console.console.print(
        f"[blue] primitive_property: Class: {this_klass.name}({this_klass.__class__.__name__}) ~{name}, {type_of}~ Nesting: {_nested_count(nesting)}")

    this_klass.add_property(name,
                            _build_type_definition(type_of, optional_type_extension, nullable))

    return rst, this_klass, up_klass, column_ct


def _add_class_relationship(model, klass, direction, cardinality, rel_to, annotation):
    model.add_klass_relation(left=klass,
                             relation=direction,
                             cardinality=cardinality,
                             right=rel_to,
                             annotation=annotation)

    return model


def _find_or_create_struct(model, name):
    struct = model.find_class_by_name(name)
    if struct:
        return struct
    return model.create_klass(name, "struct")

def _nested_count(nesting):
    return nesting.count("|")


def _add_klass_to_nesting_level(klass, klass_list: List, nesting: str):
    if (len(klass_list) - 1) < _nested_count(nesting):
        klass_list.append(klass)
    else:
        klass_list[_nested_count(nesting)] = klass
    return klass_list


def _build_type_definition(type_of: str,
                           optional_type_extension: str,
                           nullable: str,
                           type_wrapper: str = "{}",
                           format_wrapper: str = "{}") -> str:
    pyspark_type = type_to_pyspark_types.get(type_of)

    if not pyspark_type:
        breakpoint()
    pyspark_type = type_wrapper.format(f"{pyspark_type}{optional_type_extension}")
    if 'true' in nullable:
        return format_wrapper.format(f"Optional[{pyspark_type}]")
    return format_wrapper.format(pyspark_type)


def _type_is_a_struct(type_of) -> bool:
    return type_of in ["struct", "array"]


def _primitive_array(this_type_of, rst_lines):
    if not this_type_of == "array":
        return False

    next_line, _rst = fn.fst_rst(rst_lines)
    return "element" in next_line and "struct" not in next_line


def _array_of_struct(this_type_of, rst):
    if not this_type_of == "array":
        return False
    return not _primitive_array(this_type_of, rst)


def _primitive_type(type_of):
    return type_of in primitive_types


def _read_file(file_name):
    with open(Path(file_name)) as file:
        out = file.read()
    return out


rx = {
    "root": {"re": re.compile(r'^(root)'), "fn": _model_and_class},
    "column": {
        "re": re.compile(r'^.(\|--).([\w\.]*):[\s]([\w]+)([\(\)\w,]*)?\s\((nullable|containsNull).=.(true|false)\)'),
        "fn": _column_class},
    "column_prop": {
        "re": re.compile(
            r'^.([\|\s{4}]*\|--).([\w\.@]*):[\s]([\w]+)([\(\)\w,]*)?\s\((nullable|containsNull).=.(true|false)\)'),
        "fn": _column_prop
    }
}
