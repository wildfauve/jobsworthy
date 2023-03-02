from typing import Tuple, Any, Callable
import re
from functools import reduce, partial
from dataclasses import dataclass
from pathlib import Path
from rich.tree import Tree
from rich import print as rprint
from rich.table import Table

from jobsworthy.structure.puml.model import class_model
from jobsworthy.structure.puml.util import error
from jobsworthy.util import fn, console, dataklass

@dataclass
class Value(dataklass.DataClassAbstract):
    klass_model: class_model.Model
    active_class: class_model.Klass = None
    console_tree: Tree = None
    console_tree_branch_back_to: Tree = None
    console_tree_branch: Tree = None

def build(file_name: str):
    value = reduce(_parse, _read_file(file_name).split("\n"), Value(class_model.Model(), None, None))

    _print_status(value)

    return value.klass_model


def _parse(value: Value, line: str):
    func, match = _parser_finder(line)
    if not func:
        return value
    return func(value, match)


def _parser_finder(line: str):
    result = fn.remove_none(map(partial(_re_predicate, line), rx.items()))
    if not result:
        if line:
            console.console.print(f"No Match for: {line}", style="#af00ff")
        return None, None
    return result[0]


def _re_predicate(target: str, rx):
    name, options = rx
    match = options['re'].search(target)
    if not match:
        return None
    return (options['fn'], match)


def _model_start(value: Value, match):
    name, = match.groups()
    value.replace('console_tree', Tree(f"[cyan]Model Parse Tree for {name}"))
    console.console.print(f"Model Start: {name}", style="magenta")
    value.klass_model.name = name
    return value


def _model_end(value: Value, match):
    console.console.print(f"Model End", style="magenta")
    return value


def _class_start(value: Value, match):
    if value.active_class:
        raise error.TableSchemaException(
            f"Problem with class definition {value.active_class.name} not completed before starting new class")

    name, stereotype = match.groups()

    base_branch = value.console_tree.add(f"[green]{name} <<{stereotype}>>")

    value.replace('console_tree_branch', base_branch).replace('console_tree_branch_back_to', base_branch)
    # console.console.print(f"Class Start: {name}, stereotype: {stereotype}", style="cyan")

    value.replace('active_class', value.klass_model.create_klass(name, stereotype))
    return value


def _class_end(value: Value, match):
    # console.console.print(f"Class End: {value.active_class.name}", style="cyan")

    value.active_class.end_declaration()
    value.replace('active_class', None)
    return value


def _class_meta(value: Value, _match):
    value.replace('console_tree_branch', value.console_tree_branch.add("[blue]Meta"))
    # console.console.print(f"Start Class Meta for: {klass.name}", style="blue")

    value.active_class.start_meta()
    return value


def _class_behaviour(value: Value, _match):
    value.replace('console_tree_branch', value.console_tree_branch.add("[blue]Behaviour"))
    # console.console.print(f"Start Class Behaviour for: {klass.name}", style="blue")

    value.active_class.start_behaviour()
    return value


def _class_segment_end(value: Value, match):
    # console.console.print(f"End Class Segment for: {klass.name}", style="blue")
    value.replace('console_tree_branch', value.console_tree_branch_back_to)

    value.active_class.end_segment()
    return value


def _class_prop(value: Value, match):
    name, prop_type = match.groups()
    if value.active_class is None:
        raise error.TableSchemaException(f"Got property: {name} but a class is not open for definition")

    value.console_tree_branch.add(f"[yellow]{name}: [magenta]{prop_type}")

    value.active_class.add_property(name.strip(), prop_type.strip() if prop_type else prop_type)
    return value


def _relation(value: Value, match):
    side1, relation, _, cardinality, side2, *annotation = match.groups()

    # console.console.print(f"Relation: {side1} {relation} {cardinality} {side2} {annotation}", style="yellow")

    value.klass_model.add_klass_relation(left=side1,
                                         relation=relation,
                                         cardinality=cardinality,
                                         right=side2,
                                         annotation=annotation)
    return value


def _noop(value: Value, match):
    console.console.print(f"Noop of {match}")
    return value


def _read_file(file_name):
    with open(Path(file_name)) as file:
        out = file.read()
    return out


def _print_status(value: Value):
    rprint(value.console_tree)

    table = Table(title="Parse Status")

    table.add_column("Metric", justify="right", style="cyan", no_wrap=True)
    table.add_column("Count", justify="center", style="magenta")

    kls_types = _number_of_klass_types(value.klass_model)
    table.add_row("Number of Classes", str(len(value.klass_model.klasses)))
    for name, ct in kls_types.items():
        table.add_row(f"Number of Class Type: {name}", str(ct))
    table.add_row("Number of Relations", str(_number_of_relations(value.klass_model)))
    console.console.print(table)


def _number_of_relations(model):
    return reduce(_accumulate, model.klasses, 0)


def _number_of_klass_types(model):
    return reduce(_kls_types_ct, model.klasses, dict())


def _accumulate(ct, klass):
    return ct + len(klass.related_to)


def _kls_types_ct(ct, klass):
    name = klass.__class__.__name__
    if ct.get(name):
        ct[name] += 1
    else:
        ct[name] = 1
    return ct


rx = {
    "enduml": {"re": re.compile(r'@enduml'), "fn": _model_end},
    "comment": {"re": re.compile(r"\s*'"), "fn": _noop},
    "!": {"re": re.compile(r'^!'), "fn": _noop},
    "startuml": {"re": re.compile(r'@startuml\s*([\w_-]+)'), "fn": _model_start},
    "class_declaration": {"re": re.compile(r'^class\s*(\w+)\s*<{0,2}([\w-]*)>{0,2}'), "fn": _class_start},
    "class_declaration_end": {"re": re.compile(r'^\}'), "fn": _class_end},
    "class_meta": {"re": re.compile(r'\s*--meta--\s*'), "fn": _class_meta},
    "class_behaviour": {"re": re.compile(r'\s*--behaviour--\s*'), "fn": _class_behaviour},
    "relation": {
        "re": re.compile(r'(\w+)\s*(<[-.\|]+|[-.\|]+>)\s*("([\d\.\*]*)"|\s*)\s*(\w+):?\s*<{0,2}([\.\w:-]+)?>{0,2}'),
        "fn": _relation},
    "class_prop": {"re": re.compile(r'\s*([\w@-]+):?\s*([\[\]\(\),\w\/\.\s]+)?'), "fn": _class_prop},
    "class_segment_end": {"re": re.compile(r'\s*===\s*'), "fn": _class_segment_end}
}
