from typing import List, Union, Optional
from pymonad.tools import curry
from enum import Enum
from pyspark.sql import dataframe
from pyspark.sql import functions as F

from jobsworthy.util import fn

from . import repo_messages, sql_builder


class DataAgreementType(Enum):
    """
        repo.TableProperty("my_namespace:spark:table:schema:version", "0.0.1"),
        repo.TableProperty("my_namespace:spark:table:schema:partitionColumns", "identity"),
        repo.TableProperty("my_namespace:spark:table:schema:pruneColumn", "identity"),
        repo.TableProperty("my_namespace:dataProduct:port", "superTable"),
        repo.TableProperty("my_namespace:dq:updateFrequency", "daily"),
        repo.TableProperty("my_namespace:catalogue:description",
                           "Some description"),

    """
    SCHEMA = "urn:{ns}:spark:table:schema"
    SCHEMA_VERSION = f"{SCHEMA}:version"
    PARTITION_COLUMNS = f"{SCHEMA}:partitionColumns"
    PRUNE_COLUMN = f"{SCHEMA}:pruneColumn"

    DATA_PRODUCT = "urn:{ns}:dataProduct"
    PORT = f"{DATA_PRODUCT}:port"

    UPDATE_FREQUENCY = "urn:{ns}:dq:updateFrequency"

    CATALOGUE = "urn:{ns}:catalogue"
    DESCRIPTION = f"{CATALOGUE}:description"


class TableProperty:
    @classmethod
    def table_property_expression(cls, set_of_props: Optional[List]):
        if not set_of_props:
            return None
        return ",".join([prop.format_as_expression() for prop in set_of_props])

    @classmethod
    def table_property_expression_keys(cls, set_of_props: List):
        if not set_of_props:
            return None
        return ",".join([prop.format_key_as_expression() for prop in set_of_props])

    def __init__(self,
                 key: Union[str, DataAgreementType],
                 value: str,
                 ns: str = "",
                 specific_part: str = None):
        if isinstance(key, str):
            self.init_using_str_type(key, value)
        else:
            self.init_using_data_contract_type(key, value, ns, specific_part)

    def init_using_str_type(self, key: str, value: str):
        self.key = self.prepend_urn(key)
        self.value = value

    def init_using_data_contract_type(self, key: DataAgreementType,
                                      value: str,
                                      ns: str,
                                      specific_part: str = None):
        if not ns:
            raise repo_messages.namespace_not_provided()
        self.key = key.value.format(ns=ns)
        if specific_part:
            self.key = f"{self.key}:{specific_part}"
        self.value = value

    def prepend_urn(self, key):
        if key[0:3] == 'urn':
            return key
        return f"urn:{key}"

    def __key(self):
        return (self.key, self.value)

    def __hash__(self):
        return hash((self.key, self.value))

    def __eq__(self, other):
        return self.__key() == other.__key()

    def format_as_expression(self):
        return f"'{self.key}'='{self.value}'"

    def format_key_as_expression(self):
        return f"'{self.key}'"


class PropertyManager:

    def __init__(self,
                 session,
                 asserted_table_properties: List[TableProperty],
                 db_table_name: str):
        self.cached_table_properties = None
        self.session = session
        self.asserted_table_properties = asserted_table_properties
        self.db_table_name = db_table_name

    def merge_table_properties(self):
        if not self.asserted_table_properties:
            return self
        set_on_table = set(self.to_table_properties())

        self.add_to_table_properties(set(self.asserted_table_properties) - set_on_table)
        self.remove_from_table_properties(set_on_table - set(self.asserted_table_properties))
        return self

    def invalidate_table_property_cache(self):
        self.cached_table_properties = None

    def to_table_properties(self, only: List[str] = None) -> List[TableProperty]:
        props = [TableProperty(prop.key, prop.value) for prop in (self.get_table_properties()
                                                                  .filter(F.col('key').startswith('urn'))
                                                                  .select(F.col('key'), F.col('value'))
                                                                  .collect())]
        if not only:
            return props
        return fn.select(self.props_urn_filter_predicate(only), props)

    @curry(3)
    def props_urn_filter_predicate(self, only: List[str], table_prop: TableProperty):
        return table_prop.key in only

    def get_table_properties(self) -> dataframe.DataFrame:
        if self.cached_table_properties:
            return self.cached_table_properties
        self.cached_table_properties = self.session.sql(sql_builder.show_properties(self.db_table_name))
        return self.cached_table_properties

    def add_to_table_properties(self, to_add: List[TableProperty]):
        if not to_add:
            return self
        self.session.sql(sql_builder.set_properties(table_name=self.db_table_name,
                                                    props=TableProperty.table_property_expression(to_add)))

        self.invalidate_table_property_cache()
        return self

    def remove_from_table_properties(self, to_remove: List[TableProperty]):
        if not to_remove:
            return self
        self.session.sql(sql_builder.unset_properties(table_name=self.db_table_name(),
                                                      props=TableProperty.table_property_expression_keys(to_remove)))
        self.invalidate_table_property_cache()
        return self
