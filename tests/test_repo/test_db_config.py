from jobsworth.repo import db_config

def it_normalises_names_to_snake_case():

    cfg = db_config.DbConfig(db_name="aCamelCaseName",
                             data_product_name="APascalCaseName",
                             domain_name="myDomain")

    assert cfg.db_name == "a_camel_case_name"
    assert cfg.data_product_name == "a_pascal_case_name"
    assert cfg.domain_name == "my_domain"