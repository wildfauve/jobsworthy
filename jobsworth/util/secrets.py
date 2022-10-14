from typing import Union, Callable, Optional
from jobsworth import config as cfg
from . import databricks, monad, error


class Secrets:
    secrets_cache = {}

    def __init__(self,
                 config: cfg.JobConfig,
                 secrets_provider: Union[
                     databricks.DatabricksUtilMockWrapper, databricks.DatabricksUtilsWrapper],
                 scope_override: str = None):
        self.config = config
        self.secret_provider = secrets_provider
        self.scope_override = scope_override

    def get_secret(self, secret_name) -> monad.EitherMonad[Optional[str]]:
        return self.read_through(secret_name, self.on_miss_fn)

    def clear_cache(self):
        self.__class__.secrets_cache = {}
        return self

    @monad.monadic_try(error_cls=error.SecretError)
    def read_through(self, secret_name: str, read_fn: Callable):
        secret_from_cache = self.__class__.secrets_cache.get(secret_name, None)
        if secret_from_cache:
            return secret_from_cache
        secret_from_provider = self.provider().get(self.secret_scope(), secret_name)
        self.__class__.secrets_cache.update({secret_name: secret_from_provider})
        return secret_from_provider

    def on_miss_fn(self, secret_name):
        return self.provider().secrets.get(self.secret_scope(), secret_name)

    def secret_scope(self):
        """
        By convention secret scope is defined for each service in a domain, as well as including the environment.

        <domain>.<service>.<env>

        However, the default scope can be overridden with the scope_override constructor
        """
        if self.scope_override:
            return self.scope_override
        return f"{self.config.domain_name}.{self.config.service_name}.{self.config.env}"

    def provider(self):
        return self.secret_provider.utils().secrets
