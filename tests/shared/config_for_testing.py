from jobsworthy import config as cfg
from jobsworthy.util import env

DATABASE_NAME = 'my_db'
DOMAIN_NAME = "my_domain"
DATA_PRODUCT_NAME = "my_data_product"
SERVICE_NAME = "my_service"

DB_FILE_SYSTEM_PATH_ROOT = "spark-warehouse"
CHECKPOINT_ROOT = "tests/db"

SECRETS_SCOPE = "my_secrets_scope"

config = {"env": env.Env().env}


def build_job_config():
    return (
        cfg.JobConfig(
            data_product_name=DATA_PRODUCT_NAME,
            domain_name=DOMAIN_NAME,
            service_name=SERVICE_NAME,
        )
        .configure_hive_db(
            db_name=DATABASE_NAME,
            db_file_system_path_root=DB_FILE_SYSTEM_PATH_ROOT,
            checkpoint_root=CHECKPOINT_ROOT,
        )
        .configure_cosmos_db(
            account_key_name="CosmosDBAuthorizationKey",
            endpoint="https://cosmos-riskassetclass-dev.documents.azure.com:443/",
            db_name="RiskAssetClass",
            container_name="RiskAssetClass",
        )
    )
