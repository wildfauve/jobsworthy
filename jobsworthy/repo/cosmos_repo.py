from jobsworthy.repo import spark_db
from jobsworthy.util import secrets, logger


class CosmosDb:

    def __init__(self,
                 db: spark_db.Db,
                 secrets_provider: secrets.Secrets,
                 stream_reader):
        self.db = db
        self.secrets_provider = secrets_provider
        self.stream_reader = stream_reader

    def read_stream(self):
        return self.stream_reader().read_stream(self)

    def db_config(self):
        return self.db.config.cosmos_db

    def spark_config_options(self):
        #TODO: assumes the secret provider returns a Right
        return {"spark.cosmos.accountEndpoint": self.db_config().endpoint,
                "spark.cosmos.accountKey": self.get_secret(),
                "spark.cosmos.database": self.db_config().db_name,
                "spark.cosmos.container": self.db_config().container_name,
                "spark.cosmos.read.inferSchema.enabled": "true",
                "spark.cosmos.write.strategy": "ItemOverwrite",
                "spark.cosmos.read.partitioning.strategy": "Default",
                "spark.cosmos.changeFeed.mode": "Incremental"}

    def get_secret(self):
        account_key = self.secrets_provider.get_secret(self.db_config().account_key_name)
        if account_key.is_left():
            logger.info(f"Jobsworth: Failed to get account key for: {self.db_config().account_key_name}")
            return None
        return account_key.value




class CosmosStreamReader:
    format = "cosmos.oltp.changeFeed"

    def read_stream(self, repo):
        return (
            repo.db.session
            .readStream.format(self.__class__.format)
            .options(**repo.spark_config_options())
            .load()
        )
