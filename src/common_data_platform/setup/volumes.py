from pyspark.sql import SparkSession

def create_volume(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    volume_name: str,
) -> None:
    """Create a Unity Catalog managed volume."""

    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS
        {catalog_name}.{schema_name}.{volume_name}
        """
    )

def create_volumes(
    spark: SparkSession,
    catalog_name: str,
    volumes: list[dict],
) -> None:
    """Create all configured volumes."""

    for volume in volumes:
        create_volume(
            spark=spark,
            catalog_name=catalog_name,
            schema_name=volume["schema"],
            volume_name=volume["name"],
        )

def volume_exists(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    volume_name: str,
) -> bool:
    """Check whether a volume exists."""

    volumes = spark.sql(
        f"SHOW VOLUMES IN {catalog_name}.{schema_name}"
    )

    return (
        volumes
        .filter(f"volume_name = '{volume_name}'")
        .limit(1)
        .count()
        > 0
    )