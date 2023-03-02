from functools import partial
from dataclasses import dataclass

from .builder import class_model_builder
from .model import class_model
from .writer import file_writer
from .code_generator import hive_table_schema, repo_definition

from jobsworthy.util import fn, monad, dataklass


@dataclass
class MakeValue(dataklass.DataClassAbstract):
    puml_model: str = None
    schema_location: str = None
    vocab_location: str = None
    repo_location: str = None
    schema: list = None
    vocab: dict = None
    repo: list = None
    klass_model: class_model.Model = None


def make(puml_model: str,
         schema_location: str = None,
         vocab_location: str = None,
         repo_location: str = None) -> monad.EitherMonad[MakeValue]:
    """
    Entry point for the CLI
    """
    result = (
            locations(puml_model, schema_location, vocab_location, repo_location)
            >> puml_builder
            >> hive_table_code_generator
            >> repo_code_generator
            >> writer
    )

    return result


def locations(puml_model: str,
              schema_location: str = None,
              vocab_location: str = None,
              repo_location: str = None) -> monad.EitherMonad[MakeValue]:
    return monad.Right(MakeValue(puml_model=puml_model,
                                 schema_location=schema_location,
                                 vocab_location=vocab_location,
                                 repo_location=repo_location))


def puml_builder(value: MakeValue) -> monad.EitherMonad[MakeValue]:
    klass_model = class_model_builder.build(value.puml_model)
    return monad.Right(value.replace('klass_model', klass_model))


def hive_table_code_generator(value: MakeValue) -> monad.EitherMonad[MakeValue]:
    schema, vocab = hive_table_schema.generate(value.klass_model)
    value.replace('schema', schema).replace('vocab', vocab)
    return monad.Right(value)


def repo_code_generator(value: MakeValue) -> monad.EitherMonad[MakeValue]:
    repo = repo_definition.generate(value.klass_model)
    value.replace('repo', repo)
    return monad.Right(value)


def writer(value: MakeValue) -> monad.EitherMonad[MakeValue]:
    file_writer.write(value.schema_location,
                      value.vocab_location,
                      value.repo_location,
                      value.schema,
                      value.vocab,
                      value.repo)
    return monad.Right(value)
