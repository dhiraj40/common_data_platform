from pyspark.sql import SparkSession

from common_data_platform.setup.config import load_config
from common_data_platform.setup.catalog import create_catalog
from common_data_platform.setup.schemas import create_schemas
from common_data_platform.setup.volumes import create_volumes
from common_data_platform.setup.validation import validate_platform_setup


spark = SparkSession.getActiveSession()

# Environment can later come from a Databricks widget/job parameter
environment = "dev"

config = load_config(environment)

catalog_name = config["catalog_name"]
schemas = config["schemas"]
volumes = config["volumes"]


# 1. Catalog
create_catalog(
    spark=spark,
    catalog_name=catalog_name,
)

# 2. Schemas
create_schemas(
    spark=spark,
    catalog_name=catalog_name,
    schemas=schemas,
)

# 3. Volumes
create_volumes(
    spark=spark,
    catalog_name=catalog_name,
    volumes=volumes,
)

# 4. Validation
validate_platform_setup(
    spark=spark,
    catalog_name=catalog_name,
    schemas=schemas,
    volumes=volumes,
)

print(
    f"Common Data Platform setup completed successfully "
    f"for environment: {environment}"
)