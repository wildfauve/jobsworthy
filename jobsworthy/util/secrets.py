from typing import Union, Callable, Optional, List
from azure.identity import ClientSecretCredential
from jobsworthy import spark_job
from . import databricks, monad, error, logger, fn


class Secrets:
    """
    Provides a common API to obtain secrets in a Databricks (or test) environment.  Secrets are cached once retrieved.

    Call the constructor with a spark session, an instance of JobConfig, a secret provider, and an optional default
    scope name.  Note that the scope for retrieving a secret has the following priority:
    1. scope name provided to the get function.
    2. default scope name from the constructor
    3. a common scope name defined as <domain>.<dataproduct> from the JobConfig
    """
    secrets_cache = {}

    def __init__(self,
                 session,
                 config: spark_job.JobConfig,
                 secrets_provider: Union[
                     databricks.DatabricksUtilMockWrapper, databricks.DatabricksUtilsWrapper],
                 client_credentials_provider: Callable = ClientSecretCredential,
                 default_scope_name: str = None):
        self.session = session
        self.config = config
        self.secret_provider = secrets_provider
        self.default_scope_name = default_scope_name
        self.client_credentials_provider = client_credentials_provider
        self.client_credential = None

    def client_credential_grant(self):
        """
        Executes a client credentials grant using Azure AD, returning a client credential.
        Client Id and Client Secret must be available via the key vault.
        """
        return ClientCredential(self.client_credentials_provider).grant(self.config, self.get_secret)

    def get_secret(self, secret_name: str, non_default_scope_name: str = None) -> monad.EitherMonad[Optional[str]]:
        result = self.read_through(self.secret_scope(non_default_scope_name), secret_name, self.on_miss_fn)
        if result.is_left():
            logger.info(
                msg=f"Jobsworth: Failure to retrieve secret with scope: {self.secret_scope()}, key: {secret_name}",
                ctx=result.error().error())
        return result

    def clear_cache(self):
        self.__class__.secrets_cache = {}
        return self

    @monad.monadic_try(error_cls=error.SecretError)
    def read_through(self, scope: str, secret_name: str, read_through_fn: Callable):
        secret_from_cache = fn.deep_get(self.__class__.secrets_cache, [scope, secret_name])
        if secret_from_cache:
            return secret_from_cache
        secret_from_provider = read_through_fn(scope, secret_name)

        self.add_secret_to_cache(scope, secret_name, secret_from_provider)

        return secret_from_provider

    def add_secret_to_cache(self, scope, secret_name, secret):
        if fn.deep_get(self.__class__.secrets_cache, [scope]):
            self.__class__.secrets_cache[scope].update({secret_name: secret})
        else:
            self.__class__.secrets_cache.update({scope: {secret_name: secret}})

    def on_miss_fn(self, scope, secret_name):
        return self.provider().get(scope, secret_name)

    def secret_scope(self, non_default_scope_name: str = None):
        """
        By convention secret scope is defined for each service in a domain, as well as including the environment.

        <domain>.<dataproduct>

        However, the default scope can be overridden with the default_scope_name constructor
        """
        if non_default_scope_name:
            return non_default_scope_name
        if self.default_scope_name:
            return self.default_scope_name
        return f"{self.config.domain_name}.{self.config.data_product_name}"

    def provider(self):
        return self.utils().secrets

    def utils(self):
        return self.secret_provider.utils(spark_session=self.session)


class ClientCredential:

    def __init__(self, provider):
        self.provider = provider

    def grant(self, config: spark_job.JobConfig, get_secret_fn: Callable) -> monad.EitherMonad[ClientSecretCredential]:
        credentials = self.get_credentials(config, get_secret_fn)
        if any(map(monad.maybe_value_fail, credentials)):
            return monad.Left(None)
        return self.grant_request(*list(map(monad.lift, credentials)))

    @monad.monadic_try(error.SecretError)
    def grant_request(self, client_id, client_secret, tenant_id):
        return self.provider(tenant_id, client_id, client_secret)

    def get_credentials(self, config, get_secret_fn):
        return [get_secret_fn(material) for material in self.credential_material_names(config)]

    def credential_material_names(self, config: spark_job.JobConfig) -> List[str]:
        return [config.client_id_key, config.client_secret_key, config.tenant_key]
