from __future__ import annotations
from typing import List, Callable, Optional, Dict, Union, Tuple
import re
from functools import reduce, partial
from rich.console import Console

from jobsworthy.structure.puml.model import class_model
from jobsworthy.structure.puml.util import text
from jobsworthy.util import fn, error

console = Console()

STRING_TYPE_PARSER = re.compile(r'\s*(Optional)?\[?(StringType)\]?')
LONG_TYPE_PARSER = re.compile(r'\s*(Optional)?\[?(LongType)\]?')
DECIMAL_TYPE_PARSER = re.compile(r'\s*(Optional)?\[?(DecimalType\([\d,\s*]*\))\]?')
ARRAY_TYPE_PARSER = re.compile(r'\s*(Optional\[)?(ArrayType){1}\[?(DecimalType\([\d,\s*]*\)|StringType)\]*')


def generate(model: class_model.Model) -> Tuple[List, Dict]:
    table = model.find_class_by_type(class_model.Table)
    if not table:
        return model
    result = reduce(_column_generator, table.select_class_relations_by_type(class_model.Column,
                                                                            class_model.Column.sort_by_column_position),
                    _template())
    result[0].append(")")
    return result


def _column_generator(definition: List, column: class_model.Column):
    defn, vocab = definition
    defn.append(f".column()  # {column.name}")

    vocab_ns_meta = column.meta_by_name("vocabNamespace")

    vocab_ns = text.to_camel_case(vocab_ns_meta.value) if vocab_ns_meta else None

    if column.is_struct():
        defn.append(f".struct('{_vocab_namespace(text.to_camel_case(column.name), vocab_ns)}', False)")
        _extend_vocab(vocab, vocab_ns, column, text.to_camel_case)

    if not column.properties:
        raise error.TableSchemaException(f"No properties defined in class {column.name}")

    reduce(partial(_property_definition, vocab_ns), column.properties, definition)

    if column.is_struct():
        defn.append(".end_struct()")

    defn.append("\n")
    return definition


def _property_definition(vocab_ns: Optional[class_model.Meta], definition: List, prop: class_model.Property):
    matches = _sort_on_priority(fn.remove_none(map(partial(_re_predicate, prop.type_of), type_matches)))
    # console.print(matches)
    if not matches:
        raise error.TableSchemaException(f"No matching type function for type {prop.type_of}")
    return matches[0][0](vocab_ns, definition, matches[0][1], prop)


def _sort_on_priority(matches: List) -> List:
    if not matches:
        return matches
    return sorted(matches, key=lambda m: m[2])


def _array_type(vocab_ns: class_model.Meta,
                definition: List,
                matches: re.Match,
                prop: class_model.Property):
    """
    Patterns:
    + ArrayType[StringType]
    + Optional[ArrayType[StringType]]
    + ArrayType[DecimalType(20,11)]
    + Optional[ArrayType[DecimalType(20,11)]]
    """
    defn, vocab = definition
    optional, arraytype, type_def = matches.groups()

    # if "stringtype" in type_def.lower():
    #     type_of = "T.StringType"
    # elif "decimaltype" in type_def.lower():
    #     type_of = "T.DecimalType"
    # else:
    #     raise error.TableSchemaException(f"Array type of {prop.type_of} not parsable")

    defn.append(f".array('{_vocab_namespace(prop.name, vocab_ns)}', T.{type_def}, {_optional_type(optional)})")

    _extend_vocab(vocab, vocab_ns, prop)
    return definition


def _string_type(vocab_ns: class_model.Meta,
                 definition: List,
                 matches: re.Match,
                 prop: class_model.Property):
    defn, vocab = definition
    optional, type_def = matches.groups()

    defn.append(f".string('{_vocab_namespace(prop.name, vocab_ns)}', {_optional_type(optional)})")
    _extend_vocab(vocab, vocab_ns, prop)
    return definition


def _long_type(vocab_ns: class_model.Meta,
               definition: List,
               matches: re.Match,
               prop: class_model.Property):
    defn, vocab = definition
    optional, type_def = matches.groups()

    defn.append(f".long('{_vocab_namespace(prop.name, vocab_ns)}', {_optional_type(optional)})")
    _extend_vocab(vocab, vocab_ns, prop)
    return definition


def _decimal_type(vocab_ns: class_model.Meta,
                  definition: List,
                  matches: re.Match,
                  prop: class_model.Property):
    """
    .decimal("constituent.position.unitsHeld", T.DecimalType(22, 6), False)
    """
    defn, vocab = definition
    optional, type_def = matches.groups()

    defn.append(f".decimal('{_vocab_namespace(prop.name, vocab_ns)}', T.{type_def},  {_optional_type(optional)})")
    _extend_vocab(vocab, vocab_ns, prop)
    return definition


def _vocab_namespace(element: str, vocab_ns: str = None) -> str:
    if not vocab_ns:
        return element
    return f"{vocab_ns}.{element}"


def _extend_vocab(vocab: Dict,
                  vocab_ns: str,
                  prop: Union[class_model.Property, class_model.Column],
                  formatter: Callable = fn.identity) -> Dict:
    return _add_term(formatter(_vocab_namespace(prop.name, vocab_ns)), vocab, prop.name, formatter)


def _re_predicate(target: str, rx):
    regex, func, priority = rx
    match = regex.search(target)
    if not match:
        return None
    return (func, match, priority)


def _term_finder(path, vocab):
    path_array = path.split(".")
    term = fn.deep_get(vocab, path_array)
    return path_array, term


def _add_term(path, vocab, prop_name, formatter: Callable = fn.identity):
    fst, rest = fn.fst_rst(path.split("."))
    if not rest:
        vocab[formatter(fst)] = {'term': formatter(prop_name)}
        return vocab
    if not vocab.get(fst):
        vocab[formatter(fst)] = {}
    return _add_term(".".join(rest), vocab[fst], prop_name, formatter)


def _optional_type(optional) -> bool:
    if not optional:
        return False
    return True if "optional" in optional.lower() else False


def _template():
    return (
        [
            "from jobsworthy import structure as S",
            "from pyspark.sql import types as T",
            "from . import vocab",
            "\n",
            "(S.Table(vocab=vocab.vocab, vocab_directives=[S.VocabDirective.RAISE_WHEN_TERM_NOT_FOUND])"
        ],
        dict()
    )


type_matches = {
    (ARRAY_TYPE_PARSER, _array_type, 1),
    (STRING_TYPE_PARSER, _string_type, 2),
    (LONG_TYPE_PARSER, _long_type, 2),
    (DECIMAL_TYPE_PARSER, _decimal_type, 2)
}
