import re

from jobsworth.util import env

normalise_pattern = pattern = re.compile(r'(?<!^)(?=[A-Z])')

def normalise(token):
    return normalise_pattern.sub('_', token).lower()


class JobConfig:
    def __init__(self,
                 domain_name: str,
                 data_product_name: str,
                 service_name: str,
                 env: str = env.Env().env):
        self.domain_name = normalise(domain_name)
        self.data_product_name = normalise(data_product_name)
        self.service_name = normalise(service_name)
        self.env = env
        self.db = DbConfig()
        self.cosmos_db = CosmosDbConfig()

    def configure_hive_db(self, *args, **kwargs):
        self.db.configure(*args, **kwargs)
        return self

    def configure_cosmos_db(self, *args, **kwargs):
        self.cosmos_db.configure(*args, **kwargs)
        return self


class DbConfig:

    default_table_format = "delta"

    def configure(self,
                 db_name: str,
                 table_format: str = default_table_format,
                 db_file_system_path_root: str = "dbfs:/user/hive/warehouse/",
                 checkpoint_root: str = ""):
        self.db_name = normalise(db_name)
        self.table_format = table_format
        self.db_file_system_root = normalise(db_file_system_path_root)
        self.checkpoint_root = normalise(checkpoint_root)


class CosmosDbConfig:

    def configure(self,
                  account_key_name: str,
                  endpoint: str,
                  db_name: str,
                  container_name: str):
        self.account_key_name = account_key_name
        self.db_name = db_name
        self.container_name = container_name
        self.endpoint = endpoint

