from jobsworthy.repo import sql_builder


def test_create_basic_table_with_no_props_partitions():
    col_spec = 'id string, name string, pythons array<struct<id:string>>, season string, onStream string'

    sql = sql_builder.create_unmanaged_table(table_name="my_db.my_table",
                                             col_specification=col_spec,
                                             partition_clause=None,
                                             table_property_expression=None,
                                             location="/my_db_location/my_table_location")

    expected_sql = "CREATE TABLE IF NOT EXISTS my_db.my_table ( id string, name string, pythons array<struct<id:string>>, season string, onStream string ) USING DELTA LOCATION '/my_db_location/my_table_location'"

    assert sql == expected_sql

def test_create_full_table_configuration():
    col_spec = 'id string, name string, pythons array<struct<id:string>>, season string, onStream string'

    sql = sql_builder.create_unmanaged_table(table_name="my_db.my_table",
                                             col_specification=col_spec,
                                             partition_clause=('name', 'id'),
                                             table_property_expression="'urn:my_namespace:spark:table:schema:version'='0.0.1'",
                                             location="/my_db_location/my_table_location")

    expected_sql = "CREATE TABLE IF NOT EXISTS my_db.my_table ( id string, name string, pythons array<struct<id:string>>, season string, onStream string ) USING DELTA PARTITIONED BY ( name,id ) TBLPROPERTIES ( 'urn:my_namespace:spark:table:schema:version'='0.0.1' ) LOCATION '/my_db_location/my_table_location'"

    assert sql == expected_sql


def test_drop_table():
    assert sql_builder.drop_table("my_db.my_table") == "DROP TABLE IF EXISTS my_db.my_table"