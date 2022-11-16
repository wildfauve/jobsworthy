from tests.shared import tables

def test_add_table_properties_on_create(test_db):
    my_table = tables.MyHiveTable(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    props = my_table.urn_table_properties()

    assert len(props) == 1
    assert props[0].key == "urn:my_namespace:spark:table:schema:version"
    assert props[0].value == "0.0.1"


def test_doesnt_add_table_props_when_none_defined(test_db):
    my_table = tables.MyHiveTable2(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    props = my_table.urn_table_properties()

    assert not props
