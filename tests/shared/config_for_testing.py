from jobsworthy import spark_job
from jobsworthy.util import env

DATABASE_NAME = 'my_db'
DOMAIN_NAME = "my_domain"
DATA_PRODUCT_NAME = "my_data_product_name"
SERVICE_NAME = "my_service"

DB_FILE_SYSTEM_PATH_ROOT = f"domains/{DOMAIN_NAME}/data_products/{DATA_PRODUCT_NAME}"
CHECKPOINT_OVERRIDE = f"spark-warehouse/{DB_FILE_SYSTEM_PATH_ROOT}"
# DB_FILE_SYSTEM_PATH_ROOT = ""
# CHECKPOINT_OVERRIDE = f"spark-warehouse"


SECRETS_SCOPE = "my_secrets_scope"

config = {"env": env.Env().env}


def build_job_config():
    return (
        spark_job.JobConfig(
            data_product_name=DATA_PRODUCT_NAME,
            domain_name=DOMAIN_NAME,
            service_name=SERVICE_NAME,
        )
        .configure_hive_db(
            db_name=DATABASE_NAME,
            db_file_system_path_root=DB_FILE_SYSTEM_PATH_ROOT,
            db_path_override_for_checkpoint=CHECKPOINT_OVERRIDE,
        )
        .configure_cosmos_db(
            account_key_name="CosmosDBAuthorizationKey",
            endpoint="https://cosmos-riskassetclass-dev.documents.azure.com:443/",
            db_name="RiskAssetClass",
            container_name="RiskAssetClass",
        )
        .running_in_test()
    )
