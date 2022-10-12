import re

normalise_pattern = pattern = re.compile(r'(?<!^)(?=[A-Z])')


def normalise(token):
    return normalise_pattern.sub('_', token).lower()


class DbConfig:

    def __init__(self,
                 db_name: str,
                 domain_name: str,
                 data_product_name: str,
                 table_format: str = 'delta',
                 db_file_system_path_root: str = "dbfs:/user/hive/warehouse/",
                 checkpoint_root: str = ""):
        self.db_name = normalise(db_name)
        self.domain_name = normalise(domain_name)
        self.data_product_name = normalise(data_product_name)
        self.table_format = table_format
        self.db_file_system_root = normalise(db_file_system_path_root)
        self.checkpoint_root = normalise(checkpoint_root)


class TableConfig:

    def __init__(self, table_name: str):
        self.table_name = normalise(table_name)
