from jobsworth.repo import spark_db
from jobsworth.util import secrets


class CosmosDb:

    def __init__(self,
                 db: spark_db.Db,
                 secrets_provider: secrets.Secrets,
                 account_key_name: str,
                 endpoint: str,
                 db_name: str,
                 container_name: str,
                 stream_reader):
        self.db = db
        self.secrets_provider = secrets_provider
        self.account_key_name = account_key_name
        self.db_name = db_name
        self.container_name = container_name
        self.endpoint = endpoint
        self.stream_reader = stream_reader

    def read_stream(self):
        return self.stream_reader().read_stream(self)

    def spark_config_options(self):
        #TODO: assumes the secret provider returns a Right
        return {"spark.cosmos.accountEndpoint": self.endpoint,
                "spark.cosmos.accountKey": self.secrets_provider.get_secret(self.account_key_name).value,
                "spark.cosmos.database": self.db_name,
                "spark.cosmos.container": self.container_name,
                "spark.cosmos.read.inferSchema.enabled": "true",
                "spark.cosmos.write.strategy": "ItemOverwrite",
                "spark.cosmos.read.partitioning.strategy": "Default",
                "spark.cosmos.changeFeed.mode": "Incremental"}


class StreamReader:
    format = "cosmos.oltp.changeFeed"

    def read_stream(self, repo):
        return (
            repo.db.session
            .readStream.format(format)
            .options(**repo.spark_config_options())
            .load()
        )
