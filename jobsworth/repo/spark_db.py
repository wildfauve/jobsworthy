from jobsworth import config
from jobsworth.util import error, logger


class Db:

    def __init__(self, session, config: config.JobConfig):
        self.session = session
        self.config = config
        self.create_db_if_not_exists()

    def create_db_if_not_exists(self):
        self.session.sql(f"create database IF NOT EXISTS {self.database_name()}")

    def drop_db(self):
        self.session.sql(f"drop database IF EXISTS {self.database_name()} CASCADE")
        return self

    def db_exists(self) -> bool:
        return self.session.catalog.databaseExists(self.database_name())

    def table_exists(self, table_name):
        return table_name in self.list_tables()

    def catalog_table_exists(self, table_name):
        return self.session.catalog.tableExists(table_name)

    def list_tables(self):
        return [table.name for table in self.session.catalog.listTables(self.database_name())]

    def database_name(self):
        return self.config.db.db_name

    def db_table_name(self, table_name):
        return f"{self.database_name()}.{table_name}"

    def table_format(self):
        return self.config.db.table_format

    def db_file_system_path_root(self):
        return self.config.db.db_file_system_root

    def db_path(self):
        """
        The db path is required for DeltaTable.forPath operations (the delta_table function from hive_repo)
        """
        if not self.db_file_system_path_root():
            raise error.RepoConfigError(
                msg="Jobsworth: db path not configured.  Set db_file_system_path_root on the HIVE config")

        if self.db_file_system_path_root()[-1] == "/":
            logger.info(msg="Jobsworth: WARNING. db_file_system_path_root should not end with a '/'")
            return f"{self.db_file_system_path_root()[:-1]}/{self.database_name()}.db"
        return f"{self.db_file_system_path_root()}/{self.database_name()}.db"

    def table_location(self, table_name):
        """
        By default, the structure of the delta table location is as follows:

        /<domain>/<data_product>/delta/<table>
        """
        return f"{self.config.db.checkpoint_root}/{self.config.domain_name}/delta/{self.config.data_product_name}/{table_name}"

    def checkpoint_location(self, table_name):
        """
        By default, the structure of the stream checkpoint location is as follows:

        /<domain>/<data_product>/delta/<table>/_checkpoint
        """
        return f"{self.table_location(table_name)}/_checkpoint"
