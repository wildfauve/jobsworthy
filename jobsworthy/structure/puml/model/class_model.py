from __future__ import annotations
from typing import Tuple, Any, Union, Callable, List
from functools import partial
from dataclasses import dataclass, field

from jobsworthy.structure.puml.util import error, text
from jobsworthy.util import fn


@dataclass
class Model:
    name: str = None
    klasses: list = field(default_factory=list)

    def create_klass(self, name, stereotype):
        klass = Klass.factory(name=text.sanitise(name), stereotype=stereotype)
        self.klasses.append(klass)
        return klass

    def append_klass(self, klass: Klass):
        self.klasses.append(klass)
        return klass

    def add_klass_relation(self,
                           left: Union[str, Klass],
                           relation: str,
                           right: Union[str, Klass],
                           cardinality: str = "",
                           annotation: str = ""):
        left_klass, right_klass = self.find_left_right_klasses(left, right)
        if not left_klass or not right_klass:
            return self
        if ">" in relation:
            left_klass.has_relation_to(right_klass, cardinality, annotation)
        elif "<" in relation:
            right_klass.has_relation_to(left_klass, cardinality, annotation)
        return self

    def find_class_by_type(self, klass_type: Union[Table, Column, Klass]):
        return fn.find(partial(self.klass_type_predicate, klass_type), self.klasses)

    def find_left_right_klasses(self, left: Union[str, Klass], right: Union[str, Klass]) -> Tuple:
        left_k = left if isinstance(left, Klass) else self.find_class_by_name(left)
        rght_k = right if isinstance(right, Klass) else self.find_class_by_name(right)
        return (left_k, rght_k)

    def find_class_by_name(self, name):
        return fn.find(partial(self.klass_name_predicate, name), self.klasses)

    def klass_name_predicate(self, name, klass):
        return klass.name == name

    def klass_type_predicate(self, klass_type: Any, klass):
        return isinstance(klass, klass_type)

    def __repr__(self) -> str:
        s = f"Model: {self.name}\n"
        for klass in self.klasses:
            s = s + f"|_ {klass.name}: {klass.stereotype}\n"
            for prop in klass.properties:
                s = s + f"   |_ {prop.name}: {prop.type_of}\n"
        return s


@dataclass
class Klass:
    name: str
    stereotype: str
    meta: list = field(default_factory=list)
    properties: list = field(default_factory=list)
    related_to: list = field(default_factory=list)
    behaviours: list = field(default_factory=list)
    collecting_meta: bool = None
    collecting_behaviour: bool = None
    in_declaration: bool = field(default=True)

    @classmethod
    def factory(cls, name, stereotype):
        kls = cls.klass_definitions().get(stereotype)
        if not kls:
            return cls(name, stereotype)
        return kls(name, stereotype)

    @classmethod
    def klass_definitions(cls):
        return {
            "table": Table,
            "column": Column,
            "database": Database,
            "struct": Struct,
            "array_struct": ArrayStruct
        }

    def __eq__(self, other):
        return self.name == other.nam

    def not_accepting_properties(self):
        return not self.in_declaration or self.collecting_behaviour or self.collecting_meta

    def end_declaration(self):
        self.in_declaration = False
        return self

    def add_single_meta(self, name, term):
        self.meta.append(Meta(name=name, value=term))
        return self

    def start_meta(self):
        self.collecting_meta = True

    def start_behaviour(self):
        self.collecting_behaviour = True

    def end_segment(self):
        if self.collecting_meta:
            self.collecting_meta = False
        if self.collecting_behaviour:
            self.collecting_behaviour = False
        return self

    def add_property(self, name, term):
        if self.collecting_meta:
            return self.add_meta(name,term)
        if self.collecting_behaviour:
            return self.add_behaviour(name)

        prop = Property(name=name, type_of=term)
        if not self.find_property(prop):
            self.properties.append(prop)
        return self


    def add_meta(self, name, term):
        self.meta.append(Meta(name=name, value=term))
        return self

    def add_behaviour(self, name):
        self.behaviours.append(name)
        return self


    def has_relation_to(self, klass, cardinality, annotation):
        rel = Relation(klass=klass, cardinality=cardinality, annotation=annotation)
        if self.relation_present(rel):
            breakpoint()
        self.related_to.append(rel)
        return self

    def select_class_relations_by_type(self, klass_type: Union[Table, Column, Klass], sort_fn: Callable = fn.identity):
        return (sort_fn(
            map(self.klass_for_relation, fn.select(partial(self.klass_type_predicate, klass_type), self.related_to)))
        )

    def relation_present(self, rel):
        return fn.find(partial(self.rel_predicate, rel), self.related_to)

    def find_property(self, rel):
        return fn.find(partial(self.prop_predicate, rel), self.properties)


    def klass_type_predicate(self, klass_type, rel: Relation):
        return isinstance(rel.klass, klass_type)

    def rel_predicate(self, compare_rel, relationship):
        return compare_rel == relationship

    def prop_predicate(self, compare_prop, prop):
        return compare_prop == prop


    def klass_for_relation(self, relation: Relation):
        return relation.klass

    def to_puml(self) -> List:
        kls = [f"class {self.name} <<{self.stereotype}>> {{", ]
        if self.meta:
            kls.append("    --meta--")
            [kls.append(f"    {meta.name}: {meta.value}") for meta in self.meta]
            kls.append("    ===")
        if self.properties:
            [kls.append(f"    {prop.name}: {prop.type_of}") for prop in self.properties]
        kls.append("}\n")
        return kls


@dataclass
class Database(Klass):
    pass


@dataclass
class Table(Klass):
    pass


@dataclass
class Column(Klass):

    @classmethod
    def sort_by_column_position(self, klasses: List[Column]):
        return sorted(klasses, key=lambda k: k.column_position())

    def column_position(self):
        return int(self.meta_by_name("isAtColumnPosition", True).value)

    def meta_by_name(self, name, fail_when_not_found: bool = False, formatter: Callable = fn.identity):
        meta = Meta.find_by_name(name, self.meta)
        if not meta and fail_when_not_found:
            raise error.TableSchemaException(f"Column: {self.name} has no isAtColumnPosition meta value")
        return meta

    def is_struct(self):
        return len(self.properties) > 1


@dataclass
class Struct(Klass):
    pass


@dataclass
class ArrayStruct(Klass):
    pass


@dataclass
class Property:
    name: str
    type_of: str

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __hash__(self):
        return hash((self.name, self.type_of))



@dataclass
class Meta:
    name: str
    value: str
    formatter: Callable = None

    @classmethod
    def find_by_name(cls, name, meta_pairs: List[Meta]):
        return fn.find(partial(cls.meta_name_predicate, name), meta_pairs)

    @classmethod
    def meta_name_predicate(cls, name, meta: Meta):
        return meta.name == name


@dataclass
class Relation:
    klass: Klass
    annotation: list = field(default_factory=list)
    cardinality: str = field(default_factory=str)

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __hash__(self):
        return hash((self.klass.name, self.cardinality))

    def to_puml(self, left_klass: Klass) -> str:
        cardinality_term = f"\"{self.cardinality}\" " if self.cardinality else ""
        annotation_term = self.annotation if self.annotation else ""

        return f"{left_klass.name} --> {cardinality_term}{self.klass.name} {annotation_term}"
