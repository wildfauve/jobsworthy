from typing import List
from functools import partial
from dataclasses import dataclass

from .builder import print_schema_to_puml
from .model import class_model
from .writer import file_writer
from .code_generator import puml_generator

from jobsworthy.util import fn, monad, dataklass


@dataclass
class MakeValue(dataklass.DataClassAbstract):
    spark_schema_print_location: str = None
    output_to: str = None
    model_name: str = None
    puml: List = None
    klass_model: class_model.Model = None


def make(spark_schema_location: str,
         model_name: str,
         output_to: str = None) -> monad.EitherMonad[MakeValue]:
    """
    Entry point for the CLI
    """
    result = (
            locations(spark_schema_location, model_name, output_to)
            >> schema_to_class_model
            >> generate_puml
            >> writer
    )

    return result


def locations(spark_schema_print_location: str,
              model_name: str,
              output_to: str = None) -> monad.EitherMonad[MakeValue]:
    return monad.Right(MakeValue(spark_schema_print_location=spark_schema_print_location,
                                 model_name=model_name,
                                 output_to=output_to))


def schema_to_class_model(value: MakeValue) -> monad.EitherMonad[MakeValue]:
    klass_model = print_schema_to_puml.build(value.spark_schema_print_location,
                                             value.model_name)
    return monad.Right(value.replace('klass_model', klass_model))


def generate_puml(value: MakeValue) -> monad.EitherMonad[MakeValue]:
    puml = puml_generator.generate(value.klass_model)
    value.replace('puml', puml)
    return monad.Right(value)


def writer(value: MakeValue) -> monad.EitherMonad[MakeValue]:
    if value.output_to:
        file_writer.general_writer([(value.output_to, value.puml)])
    return monad.Right(value)
