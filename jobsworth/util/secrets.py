from . import fn


class SecretsProvider:

    def __init__(self, config, databricks_utils_wrapper):
        self.config = config
        self.databricks_utils_wrapper = databricks_utils_wrapper
        self.cosmos_secret = None

    def get_cosmos_secret(self):
        if self.cosmos_secret:
            return self.cosmos_secret
        self.cosmos_secret = self.databricks_utils().secrets.get(self.secret_scope(), self.cosmos_secret_name())
        return self.cosmos_secret

    def secret_scope(self):
        return fn.deep_get(self.config, ['secret_scope'])

    def cosmos_secret_name(self):
        return fn.deep_get(self.config, ['cosmos_secret_name'])

    def databricks_utils(self):
        return self.databricks_utils_wrapper.utils()
