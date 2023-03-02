from typing import List, Callable, Optional, Dict, Union, Tuple
from functools import reduce, partial
import sys

from jobsworthy.structure.puml.model import class_model
from jobsworthy import repo
from jobsworthy.util import fn, error


"""
from jobsworthy import repo

class MyDb(repo.Db):
    db_properties = [
        repo.DbProperty(repo.DataAgreementType.DATA_PRODUCT, "my_dp", "my_prefix"),
        repo.DbProperty(repo.DataAgreementType.DESCRIPTION,
                        "DB Descirption", "my_prefix"),
    ]

class MyTable(repo.HiveRepo):
    table_name = "MyTable"

    table_properties = [
        repo.TableProperty(repo.DataAgreementType.DATA_PRODUCT, "my_dp", "my_prefix"),
        repo.TableProperty(repo.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_prefix"),
        repo.TableProperty(repo.DataAgreementType.PARTITION_COLUMNS, "identity", "my_prefix"),
        repo.TableProperty(repo.DataAgreementType.PRUNE_COLUMN, "identity", "my_prefix"),
        repo.TableProperty(repo.DataAgreementType.PORT, "superTable", "my_prefix"),
        repo.TableProperty(repo.DataAgreementType.UPDATE_FREQUENCY, "daily", "my_prefix"),
        repo.TableProperty(repo.DataAgreementType.DESCRIPTION, "My Table Description.", "my_prefix"),
    ]

    partition_columns = ("partitionColumn",)

    pruning_column = "IdentityColumn"
"""
def generate(model: class_model.Model) -> Tuple[List, Dict]:
    code = reduce(partial(_class_type_generator, model), [class_model.Database, class_model.Table], _template())
    return code


def _class_type_generator(model: class_model.Model, code: List, klass: class_model.Klass):
    instance = model.find_class_by_type(klass)
    if not instance or not instance.stereotype:
        return code
    code.append("\n")
    return getattr(sys.modules[__name__], f"_generate_{instance.stereotype}_definition")(code, instance)


def _generate_database_definition(code, db: class_model.Database):
    code.append(f"class {db.name}(repo.Database):")
    code.append(f"    pass")
    return code


def _generate_table_definition(code, table: class_model.Table):
    code.append(f"class {table.name}(repo.HiveRepo):")
    code.append(f"    pass")
    return code

def _template() -> List:
    return ["from jobsworthy import repo"]