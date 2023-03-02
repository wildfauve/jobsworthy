from __future__ import annotations
from typing import List, Callable, Optional, Dict, Union, Tuple
import re
from functools import reduce, partial
from rich.console import Console

from jobsworthy.structure.puml.model import class_model
from jobsworthy.util import fn, error

console = Console()


def puml_template():
    def inner(f):
        def template(*args, **kwargs):
            result = f(*args, **{**kwargs, **{'template': start_template(args[0])}})
            result.append("@enduml")
            return result

        def start_template(model):
            return [
                f"@startuml {model.name}\n",
                "!theme amiga\n",
            ]

        return template

    return inner


@puml_template()
def generate(model: class_model.Model, template: List = None) -> List:
    return _reducer(_relationship_generator, model.klasses,
                    _reducer(_class_generator, model.klasses, template))



def _reducer(reduce_fn: Callable, xs, accumulator):
    return reduce(reduce_fn, xs, accumulator)


def _class_generator(code, klass):
    return code + klass.to_puml()


def _relationship_generator(code, klass):
    return code + list(map(lambda rel: rel.to_puml(left_klass=klass), klass.related_to))


def _template(model_name):
    return [
        f"@startuml {model_name}"
    ]
