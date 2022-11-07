import json
from jobsworthy.util import databricks

# def test_mock_file_system():
#     db_wrapper = databricks.DatabricksUtilMockWrapper(None, None)
#
#     with db_wrapper.utils().fs.open("tests/fixtures/table1_rows.json", 'r') as file:
#         json_file = json.loads(file.read())
#
#     assert json_file
#
# def test_databricks_fs(test_container):
#     db_wrapper = databricks.DatabricksUtilsWrapper(test_container.session())
#
#
