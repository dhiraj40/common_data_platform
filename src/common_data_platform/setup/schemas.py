from pyspark.sql import SparkSession

def create_schema(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
) -> None:
    """Create a schema if it does not already exist."""

    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
        """
    )

def create_schemas(
    spark: SparkSession,
    catalog_name: str,
    schemas: list[str],
) -> None:
    """Create all configured schemas."""

    for schema_name in schemas:
        create_schema(
            spark=spark,
            catalog_name=catalog_name,
            schema_name=schema_name,
        )

def schema_exists(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
) -> bool:
    """Check whether a schema exists."""

    schemas = spark.sql(
        f"SHOW SCHEMAS IN {catalog_name}"
    )

    return (
        schemas
        .filter(f"databaseName = '{schema_name}'")
        .limit(1)
        .count()
        > 0
    )