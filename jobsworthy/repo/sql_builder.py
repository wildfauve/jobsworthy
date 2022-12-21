from typing import Tuple, List


def create_unmanaged_table(table_name: str,
                           col_specification: str,
                           location: str,
                           partition_clause: Tuple = None,
                           table_property_expression: str = None):
    sql = base_create_table(table_name=table_name,
                            col_specification=col_specification,
                            partition_clause=partition_clause,
                            table_property_expression=table_property_expression)
    sql.append(f"LOCATION '{location}'")
    return joiner(sql)


def create_managed_table(table_name: str,
                         col_specification: str,
                         partition_clause: Tuple = None,
                         table_property_expression: str = None):
    return joiner(base_create_table(table_name=table_name,
                                    col_specification=col_specification,
                                    partition_clause=partition_clause,
                                    table_property_expression=table_property_expression))


def base_create_table(table_name: str,
                      col_specification: str,
                      partition_clause: Tuple = None,
                      table_property_expression: str = None) -> List[str]:
    tbl_props = f"TBLPROPERTIES ( {table_property_expression} )" if table_property_expression else None
    partition_on = f"PARTITIONED BY ( {','.join(partition_clause)} )" if partition_clause else None

    sql = [f"CREATE TABLE IF NOT EXISTS {table_name} ( {col_specification} ) USING DELTA"]
    sql.append(partition_on) if partition_on else sql
    sql.append(tbl_props) if tbl_props else sql
    return sql


def drop_table(table_to_drop):
    return joiner([f"DROP TABLE IF EXISTS {table_to_drop}"])


def show_properties(table_name):
    return joiner([f"SHOW TBLPROPERTIES {table_name}"])


def set_properties(table_name: str, props: str):
    return joiner([f"alter table {table_name} SET TBLPROPERTIES ({props})"])


def unset_properties(table_name: str, props: str):
    return joiner([f"alter table {table_name} UNSET TBLPROPERTIES ({props})"])


def joiner(expr: List):
    return " ".join(expr)
