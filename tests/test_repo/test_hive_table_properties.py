from jobsworthy import repo

from tests.shared import tables


def test_build_property_from_urn_strings():
    prop = repo.TableProperty("my_namespace:spark:table:schema:version", "0.0.1")

    assert prop.key == "urn:my_namespace:spark:table:schema:version"
    assert prop.value == "0.0.1"


def test_build_property_from_data_agreement_type():
    prop = repo.TableProperty(repo.DataAgreementType.SCHEMA_VERSION, "0.0.1", "my_namespace")
    assert prop.key == "urn:my_namespace:spark:table:schema:version"
    assert prop.value == "0.0.1"


def test_build_property_from_data_agreement_type_using_specific_part():
    prop = repo.TableProperty(key=repo.DataAgreementType.SCHEMA,
                              value="0.0.1",
                              ns="my_namespace",
                              specific_part="myVersion:version")
    assert prop.key == "urn:my_namespace:spark:table:schema:myVersion:version"
    assert prop.value == "0.0.1"


def test_add_table_properties_on_create(test_db):
    my_table = tables.MyHiveTable(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    props = my_table.property_manager.to_table_properties()

    assert len(props) == 1
    assert props[0].key == "urn:my_namespace:spark:table:schema:version"
    assert props[0].value == "0.0.1"


def test_find_table_props_by_urn(test_db):
    my_table = tables.MyHiveTableWithCallbacks(db=test_db)

    props = my_table.property_manager.to_table_properties()

    assert len(props) == 6
    expected_keys = ['urn:my_namespace:catalogue:description', 'urn:my_namespace:dataProduct:port',
                     'urn:my_namespace:dq:updateFrequency', 'urn:my_namespace:spark:table:schema:partitionColumns',
                     'urn:my_namespace:spark:table:schema:pruneColumn', 'urn:my_namespace:spark:table:schema:version']

    assert [p.key for p in props] == expected_keys


def test_doesnt_add_table_props_when_none_defined(test_db):
    my_table = tables.MyHiveTable2(db=test_db)
    my_table.write_append(tables.my_table_df(test_db))

    props = my_table.property_manager.to_table_properties()

    assert not props
