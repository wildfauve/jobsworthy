import re

from jobsworthy.util import env, error

normalise_pattern = pattern = re.compile(r'(?<!^)(?=[A-Z])')

def normalise(token):
    if not token:
        return token
    return normalise_pattern.sub('_', token).lower()

class Config:
    def __init__(self,
                 domain_name: str = None,
                 data_product_name: str = None,
                 service_name: str = None,
                 client_id_key: str = None,
                 client_secret_key: str = None,
                 tenant_key: str = None,
                 env: str = env.Env().env):
        self.domain_name = normalise(domain_name)
        self.data_product_name = normalise(data_product_name)
        self.service_name = normalise(service_name)
        self.client_id_key = client_id_key
        self.client_secret_key = client_secret_key
        self.tenant_key = tenant_key
        self.env = env
        self.db = DbConfig()
        self.cosmos_db = CosmosDbConfig()
        self.is_running_in_test = False

    def configure_hive_db(self, *args, **kwargs):
        self.db.configure(*args, **kwargs)
        return self

    def configure_cosmos_db(self, *args, **kwargs):
        self.cosmos_db.configure(*args, **kwargs)
        return self

    def running_in_test(self):
        self.is_running_in_test = True
        return self


class JobConfig(Config):
    pass



class DbConfig:

    default_table_format = "delta"

    def configure(self,
                 db_name: str,
                 db_file_system_path_root: str = None,
                 table_format: str = default_table_format,
                 checkpoint_root: str = None,
                 db_path_override_for_checkpoint: str = None):
        self.db_name = normalise(db_name)
        self.table_format = table_format
        self.db_file_system_root = normalise(db_file_system_path_root)
        self.checkpoint_root = checkpoint_root
        self.db_path_override_for_checkpoint = normalise(db_path_override_for_checkpoint)
        if self.checkpoint_root:
            raise error.RepoConfigError(
                message="Jobsworthy: use of checkpoint_root no longer supported.  Use db_path_override_for_checkpoint.")



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

