from jobsworthy import repo

from tests.shared import tables


def test_build_property_from_data_agreement_type():
    prop = repo.DbProperty(repo.DataAgreementType.DATA_PRODUCT, "my_data_product", "my_namespace")
    assert prop.key == "urn:my_namespace:dataProduct"
    assert prop.value == "my_data_product"


def test_add_db_properties_on_create(test_db):
    props = test_db.asserted_properties()

    assert len(props) == 2

    prop_keys = {p.key for p in test_db.asserted_properties()}
    assert prop_keys == {'urn:my_namespace:dataProduct', 'urn:my_namespace:catalogue:description'}

    prop_values = {p.value for p in test_db.asserted_properties()}
    assert prop_values == {'my_data_product', 'DB for Data Product 1'}


def test_get_custom_props_from_hive_db(test_db):
    row = test_db.properties.custom_properties()

    expected = '((urn:my_namespace:catalogue:description,DB for Data Product 1), (urn:my_namespace:dataProduct,my_data_product))'

    assert row.info_value == expected


def test_db_properties_as_properties(test_db):
    props = test_db.properties.to_properties()

    assert len(props) == 2

    expected_props = {('urn:my_namespace:dataProduct', 'my_data_product'),
                      ('urn:my_namespace:catalogue:description', 'DB for Data Product 1')}

    assert {(p.key, p.value) for p in props} == expected_props


def test_db_all_properties_as_properties(test_db):
    props = test_db.properties.to_all_properties()

    assert len(props) == 6

    expected_keys = {'Owner', 'urn:my_namespace:dataProduct', 'Location', 'Namespace Name',
                     'urn:my_namespace:catalogue:description', 'Comment'}

    assert {p.key for p in props} == expected_keys


def test_doesnt_add_db_props_when_none_defined(test_db_without_props):
    props = test_db_without_props.properties.to_properties()

    assert not props


def test_custom_property_regex():
    pm = repo.DbPropertyManager(session="dummy_session", asserted_properties=None, db_name="db")

    r1 = pm.split_into_key_pairs(
        '(urn:ns:catalogue:description,DB for Data Product 1), (urn:ns:dataProduct,my_data_product)')

    assert r1 == ['',
                  '(urn:ns:catalogue:description,DB for Data Product 1)',
                  ', ',
                  '(urn:ns:dataProduct,my_data_product)',
                  '']

    r2 = pm.split_into_key_pairs(
        '(urn:ns:catalogue:description,DB for Data Product #1), (urn:ns:dataProduct,myDataProduct#1)')

    assert r2 == ['',
                  '(urn:ns:catalogue:description,DB for Data Product #1)',
                  ', ',
                  '(urn:ns:dataProduct,myDataProduct#1)',
                  '']

    r3 = pm.split_into_key_pairs(
        '(urn:ns:catalogue:description,$DB$ for Data Product *#1), (urn:ns:dataProduct,my$Data$Product*#1)')

    assert r3 == ['',
                  '(urn:ns:catalogue:description,$DB$ for Data Product *#1)',
                  ', ',
                  '(urn:ns:dataProduct,my$Data$Product*#1)',
                  '']


def test_custom_property_kv_regex_noop():
    pm = repo.DbPropertyManager(session="dummy_session", asserted_properties=None, db_name="db")

    assert not pm.parse_custom_property(', ')
    assert not pm.parse_custom_property('')
    assert not pm.parse_custom_property('abc')
    assert not pm.parse_custom_property('&^%$')


def test_custom_property_kv_regex():
    pm = repo.DbPropertyManager(session="dummy_session", asserted_properties=None, db_name="db")

    result = pm.parse_custom_property('(urn:ns:catalogue:description,DB for Data Product 1)')
    assert result.key == 'urn:ns:catalogue:description'
    assert result.value == 'DB for Data Product 1'

    result = pm.parse_custom_property('(urn:ns:catalogue:description,DB for Data Product #1)')
    assert result.key == 'urn:ns:catalogue:description'
    assert result.value == 'DB for Data Product #1'

    result = pm.parse_custom_property('(urn:ns:catalogue:description,$DB$ for Data_Product *#1)')
    assert result.key == 'urn:ns:catalogue:description'
    assert result.value == '$DB$ for Data_Product *#1'
