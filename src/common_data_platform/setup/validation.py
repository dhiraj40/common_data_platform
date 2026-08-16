from pyspark.sql import SparkSession

from common_data_platform.setup.catalog import catalog_exists
from common_data_platform.setup.schemas import schema_exists
from common_data_platform.setup.volumes import volume_exists


def validate_catalog(
    spark: SparkSession,
    catalog_name: str,
) -> None:
    if not catalog_exists(spark, catalog_name):
        raise RuntimeError(
            f"Catalog validation failed: {catalog_name} does not exist."
        )


def validate_schemas(
    spark: SparkSession,
    catalog_name: str,
    schemas: list[str],
) -> None:
    missing_schemas = [
        schema_name
        for schema_name in schemas
        if not schema_exists(
            spark=spark,
            catalog_name=catalog_name,
            schema_name=schema_name,
        )
    ]

    if missing_schemas:
        raise RuntimeError(
            f"Schema validation failed. Missing schemas: {missing_schemas}"
        )


def validate_volumes(
    spark: SparkSession,
    catalog_name: str,
    volumes: list[dict],
) -> None:
    missing_volumes = []

    for volume in volumes:
        exists = volume_exists(
            spark=spark,
            catalog_name=catalog_name,
            schema_name=volume["schema"],
            volume_name=volume["name"],
        )

        if not exists:
            missing_volumes.append(
                f'{volume["schema"]}.{volume["name"]}'
            )

    if missing_volumes:
        raise RuntimeError(
            f"Volume validation failed. Missing volumes: {missing_volumes}"
        )

def table_exists(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> bool:
    tables = spark.sql(
        f"SHOW TABLES IN {catalog_name}.{schema_name}"
    )

    return (
        tables
        .filter(f"tableName = '{table_name}'")
        .limit(1)
        .count()
        > 0
    )


def validate_tables(
    spark: SparkSession,
    catalog_name: str,
    table_definitions: list[dict],
) -> None:
    missing_tables = []

    for table in table_definitions:
        if not table_exists(
            spark=spark,
            catalog_name=catalog_name,
            schema_name=table["schema"],
            table_name=table["name"],
        ):
            missing_tables.append(
                f"{table['schema']}.{table['name']}"
            )

    if missing_tables:
        raise RuntimeError(
            f"Table validation failed. Missing tables: {missing_tables}"
        )

def validate_platform_setup(
    spark: SparkSession,
    catalog_name: str,
    schemas: list[str],
    volumes: list[dict],
    table_definitions: list[dict] | None = None,
) -> None:
    validate_catalog(
        spark=spark,
        catalog_name=catalog_name,
    )

    validate_schemas(
        spark=spark,
        catalog_name=catalog_name,
        schemas=schemas,
    )

    validate_volumes(
        spark=spark,
        catalog_name=catalog_name,
        volumes=volumes,
    )

    if table_definitions:
        validate_tables(
            spark=spark,
            catalog_name=catalog_name,
            table_definitions=table_definitions,
        )