from typing import Optional, Dict
from pyspark.sql import types as T, SparkSession
from jobsworthy.repo import spark_db
from jobsworthy.util import secrets, logger


class CosmosDb:
    """
    Repository Class which wraps Cosmos Table functions.  Designed specifically to enable streaming reads.

    To create an object of this class:

    MyCosmosTable(db=db,
                  secrets_provider=secrets_provider,
                  stream_reader=repo.CosmosStreamReader)

    To create an instance for testing (where there is no access to a Cosmos table, use a mock Stream Reader

    Class Attributes
    ----------------

    `additional_spark_options`:  A Dict which is added to the default spark options.  Common options to consider adding here
                                 are as follows:
                                 {"spark.cosmos.read.inferSchema.enabled": "true",
                                  "spark.cosmos.write.strategy": "ItemOverwrite",
                                  "spark.cosmos.read.partitioning.strategy": "Default",
                                  "spark.cosmos.changeFeed.mode": "Incremental"}
                                  If providing a schema the `spark.cosmos.read.inferSchema.enabled` option is not required.

    Extension Functions
    -------------------

    `read_schema_as_dict`.  Provide an optional schema to be applied to the streaming read.  This function should return
                            a dictionary in a format that can be transformed into a pyspark.sql.types.StructType



    """

    def __init__(self,
                 db: spark_db.Db,
                 secrets_provider: secrets.Secrets,
                 stream_reader):
        self.db = db
        self.secrets_provider = secrets_provider
        self.stream_reader = stream_reader

    # Cosmos Table Attributes
    def _read_schema(self) -> Optional[Dict]:
        return self.read_schema_as_dict() if hasattr(self, "read_schema_as_dict") else None

    def _extended_spark_options(self) -> Dict:
        return self.__class__.additional_spark_options if hasattr(self, 'additional_spark_options') else {}

    # Schema
    def _schema_struct(self) -> Optional[T.StructType]:
        schema = self._read_schema()
        if not schema:
            return None
        return T.StructType.fromJson(schema)

    # Stream Processing
    def read_stream(self):
        return self.stream_reader().read_stream(session=self.db.session,
                                                spark_options=self._spark_config_options(),
                                                read_schema=self._schema_struct())

    # Configuration
    def db_config(self):
        return self.db.config.cosmos_db

    def _spark_config_options(self):
        # TODO: assumes the secret provider returns a Right
        return {**self.default_spark_options(), **self._extended_spark_options()}

    def default_spark_options(self) -> Dict:
        return {"spark.cosmos.accountEndpoint": self.db_config().endpoint,
                "spark.cosmos.accountKey": self._get_secret(),
                "spark.cosmos.database": self.db_config().db_name,
                "spark.cosmos.container": self.db_config().container_name}

    def _get_secret(self):
        account_key = self.secrets_provider.get_secret(self.db_config().account_key_name)
        if account_key.is_left():
            logger.info(f"Jobsworth: Failed to get account key for: {self.db_config().account_key_name}")
            return None
        return account_key.value


class CosmosStreamReader:
    format = "cosmos.oltp.changeFeed"

    def read_stream(self,
                    session: SparkSession,
                    spark_options: Dict,
                    read_schema: T.StructType):
        stream = (session
                  .readStream.format(self.__class__.format)
                  .options(**spark_options))

        if read_schema:
            stream.schema(read_schema)

        return stream.load()
