import pytest


class MockCosmosDBStreamReader:

    db_table_name = ""

    def read_stream(self, repo):
        return (repo.db.session
                .readStream
                .format("delta")
                .option("ignoreChanges", True)
                .table(self.__class__.db_table_name))
