from typing import Optional, Dict, List, Union, Tuple
from pyspark.sql import types as T, SparkSession

from . import spark_repo, writers, readers, spark_util, repo_messages

from jobsworthy.repo import spark_db
from jobsworthy.util import secrets, logger, monad, error


class CosmosDb(spark_repo.SparkRepo):
    """
    Repository Class which wraps Cosmos Table functions.  Designed specifically to enable streaming reads.

    To create an object of this class::

        MyCosmosTable(db=db,
                      secrets_provider=secrets_provider,
                      stream_reader=repo.CosmosStreamReader)

    To create an instance for testing (where there is no access to a Cosmos table, use a mock Stream Reader

    Class Attributes
    ----------------

    `additional_spark_options`:  A List of repo.Option which is added to the default spark options.
        Common options to consider adding here are as follows:
        [
            repo.Option.COSMOS_INFER_SCHEMA,
            repo.Option.COSMOS_ITEM_OVERWRITE,
            repo.Option.COSMOS_READ_PARTITION_DEFAULT,
            repo.Option.COSMOS_CHANGE_FEED_INCREMENTAL
        ]
        If providing a schema the `spark.cosmos.read.inferSchema.enabled` option is not required.

    """

    additional_spark_options = None

    def __init__(self,
                 db: spark_db.Db,
                 reader: Optional[spark_repo.ReaderType] = None,
                 stream_reader: Optional[spark_repo.StreamReaderType] = None,
                 stream_writer: Optional[spark_repo.StreamWriterType] = None,
                 secrets_provider: Optional[secrets.Secrets] = None):
        super().__init__(db, reader, None, stream_reader, stream_writer, None, secrets_provider)

    def _extended_spark_options(self) -> Dict:
        if not self.__class__.additional_spark_options:
            return {}
        if isinstance(self.__class__.additional_spark_options, dict):
            raise repo_messages.cosmos_additional_spark_options_as_dict_no_longer_supported()
        return spark_util.SparkOption.function_based_options(self.__class__.additional_spark_options)

    # Stream Processing
    def read_stream(self, reader_options: Optional[List[readers.ReaderSwitch]] = None):
        return self.stream_reader().read(self, reader_options=reader_options)

    # Configuration
    def db_config(self):
        return self.db.config.cosmos_db

    def spark_config_options(self) -> monad.Right:
        # TODO: assumes the secret provider returns a Right
        try_default = self.default_spark_options()
        if try_default.is_left():
            return try_default
        return monad.Right({**try_default.value, **self._extended_spark_options()})

    def default_spark_options(self) -> monad.EitherMonad[Union[Dict, Exception]]:
        db_connect = self._db_connection()
        if db_connect.is_left():
            return db_connect

        endpoint, acct_key = db_connect.value

        return monad.Right({"spark.cosmos.accountEndpoint": endpoint,
                            "spark.cosmos.accountKey": acct_key,
                            "spark.cosmos.database": self.db_config().db_name,
                            "spark.cosmos.container": self.db_config().container_name})

    def _db_connection(self) -> monad.EitherMonad[Tuple[str, str]]:
        """
        There are 2 strategies for retrieving the cosmosdb endpoint and account key.
            # They are separately configured in the config endpoint and account_key_name props
            # A single connection string is provided via a get_secret request.
        :return:
        """
        if self.db_config().account_key_name and self.db_config().endpoint:
            try_secret = self._get_secret(self.db_config().account_key_name)
            if try_secret.is_left():
                return try_secret
            return monad.Right((self.db_config().endpoint, try_secret.value))

        try_connection = self._get_secret(self.db_config().connection_string_key_name)
        if try_connection.is_left():
            return try_connection

        endpoint, key_string = try_connection.value.split(";")[0:2]

        return monad.Right((endpoint.replace('AccountEndpoint=', ''),
                            key_string.replace('AccountKey=', '')))

    def _get_secret(self, secret_name) -> monad.EitherMonad:
        account_key = self.secrets_provider.get_secret(secret_name)
        if account_key.is_left():
            logger.info(f"Jobsworth: Failed to get account key for: {secret_name}")
        return account_key
