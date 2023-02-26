from typing import List, Union, Optional, Set, Tuple
import re
from pymonad.tools import curry
from enum import Enum
from pyspark.sql import dataframe
from pyspark.sql import functions as F

from jobsworthy.util import fn

from . import repo_messages, sql_builder

db_prop_props_regex = r'(\([\s\#\$\*a-zA-Z0-9:,_-]+\))'
db_prop_kv_regex = r'\((.+),(.+)\)'


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


class Property:

    @classmethod
    def property_expression(cls, set_of_props: Optional[List]):
        if not set_of_props:
            return None
        return ",".join([prop.format_as_expression() for prop in set_of_props])

    def __init__(self,
                 key: Union[str, DataAgreementType],
                 value: str,
                 ns: str = "",
                 specific_part: str = None,
                 convert_to_urn: bool = True):
        self.key = None
        self.value = None
        if isinstance(key, str):
            self.init_using_str_type(key, value, convert_to_urn)
        else:
            self.init_using_data_contract_type(key, value, ns, specific_part)

    def init_using_str_type(self, key: str, value: str, convert_to_urn: bool):
        self.key = self.prepend_urn(key) if convert_to_urn else key
        self.value = value

    def init_using_data_contract_type(self,
                                      key: DataAgreementType,
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


class DbProperty(Property):
    pass


class TableProperty(Property):
    @classmethod
    def table_property_expression(cls, set_of_props: Optional[Union[List, Set]]):
        if not set_of_props:
            return None
        return ",".join([prop.format_as_expression() for prop in set_of_props])

    @classmethod
    def table_property_expression_keys(cls, set_of_props: Union[List, Set]):
        if not set_of_props:
            return None
        return ",".join([prop.format_key_as_expression() for prop in set_of_props])


class PropertyManager:

    def __init__(self,
                 session,
                 asserted_properties: List[Property]):
        self.cached_properties = None
        self.asserted_properties = asserted_properties
        self.session = session

    def invalidate_property_cache(self):
        self.cached_properties = None


class DbPropertyManager(PropertyManager):

    def __init__(self,
                 session,
                 asserted_properties: List[TableProperty],
                 db_name: str):
        self.db_name = db_name
        self.name_field = None
        self.value_field = None
        super().__init__(session, asserted_properties)

    def to_properties(self) -> List[DbProperty]:
        """
        Builds the custom properties only.
        :return:
        """
        return self.parse_custom_properties(self.custom_properties())

    def to_all_properties(self) -> List[DbProperty]:
        return ([DbProperty(key=getattr(prop, self.name_field), value=getattr(prop, self.value_field),
                            convert_to_urn=False) for prop in
                 self.standard_properties()]
                + self.to_properties())

    def standard_properties(self) -> List[dataframe.Row]:
        return (self.properties_from_db().filter(F.col(self.name_field) != "Properties")
                .collect())

    def parse_custom_properties(self, row: dataframe.Row) -> List[DbProperty]:
        return fn.remove_none(
            [self.parse_custom_property(prop) for prop in
             self.split_into_key_pairs(getattr(row, self.value_field)[1:-1])])

    def split_into_key_pairs(self, prop_str: str) -> List[str]:
        return re.split(db_prop_props_regex, prop_str)

    def parse_custom_property(self, prop: str) -> Optional[DbProperty]:
        if (not prop) or (not any(test_str in prop for test_str in [',', '(', ')'])):
            return None

        kv_groups = re.search(db_prop_kv_regex, prop)

        if not kv_groups:
            return None

        k, *v = kv_groups.groups()
        if not v:
            return DbProperty(key=k, value=None)
        return DbProperty(key=k, value=','.join(v))

    def custom_properties(self) -> dataframe.Row:
        """
        DB Properties, unlike table properties are serialised into a single properties column returned from the
        describe db SQL command.  The format of the column is as follows:

        Row(info_name='Properties', info_value='((urn:my_namespace:catalogue:description,DB for Data Product 1), (urn:my_namespace:dataProduct,my_data_product))')

        Returns the Dataframe Row called 'Properties'
        :return:
        """
        return self.properties_from_db().filter(F.col(self.name_field) == "Properties").collect()[0]

    def properties_from_db(self) -> dataframe.DataFrame:
        """
        For some reason a db in the spark catalog has the columns database_description_item and
        database_description_value, while the default catalogue (used in testing) has the column names info_name
        and info_value

        :return:
        """
        if self.cached_properties:
            return self.cached_properties
        self.cached_properties = self.session.sql(sql_builder.describe_db(self.db_name))

        if any(['info_' in column_name for column_name in self.cached_properties.columns]):
            self.name_field = 'info_name'
            self.value_field = 'info_value'
        else:
            self.name_field = 'database_description_item'
            self.value_field = 'database_description_value'

        return self.cached_properties


class TablePropertyManager(PropertyManager):

    def __init__(self,
                 session,
                 asserted_properties: List[TableProperty],
                 db_table_name: str):
        self.db_table_name = db_table_name
        super().__init__(session, asserted_properties)

    def merge_table_properties(self):
        if not self.asserted_properties:
            return self
        set_on_table = set(self.to_table_properties())

        self.add_to_table_properties(set(self.asserted_properties) - set_on_table)
        self.remove_from_table_properties(set_on_table - set(self.asserted_properties))
        return self

    def to_table_properties(self, only: List[str] = None) -> List[TableProperty]:
        props = [TableProperty(prop.key, prop.value) for prop in (self.get_table_properties()
                                                                  .filter(F.col('key').startswith('urn'))
                                                                  .select(F.col('key'), F.col('value'))
                                                                  .collect())]
        if not only:
            return props
        return list(fn.select(self.props_urn_filter_predicate(only), props))

    @curry(3)
    def props_urn_filter_predicate(self, only: List[str], table_prop: TableProperty):
        return table_prop.key in only

    def get_table_properties(self) -> dataframe.DataFrame:
        if self.cached_properties:
            return self.cached_properties
        self.cached_properties = self.session.sql(sql_builder.show_properties(self.db_table_name))
        return self.cached_properties

    def add_to_table_properties(self, to_add: Set[TableProperty]):
        if not to_add:
            return self
        self.session.sql(sql_builder.set_properties(table_name=self.db_table_name,
                                                    props=TableProperty.table_property_expression(to_add)))

        self.invalidate_property_cache()
        return self

    def remove_from_table_properties(self, to_remove: Set[TableProperty]):
        if not to_remove:
            return self
        self.session.sql(sql_builder.unset_properties(table_name=self.db_table_name,
                                                      props=TableProperty.table_property_expression_keys(to_remove)))
        self.invalidate_property_cache()
        return self
