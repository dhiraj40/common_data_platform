from pyspark.sql import SparkSession


def create_catalog(
    spark: SparkSession,
    catalog_name: str,
) -> None:
    """
    Create the Unity Catalog catalog if it does not already exist.
    """

    spark.sql(
        f"""
        CREATE CATALOG IF NOT EXISTS {catalog_name}
        """
    )


def catalog_exists(
    spark: SparkSession,
    catalog_name: str,
) -> bool:
    """
    Check whether a catalog exists.
    """

    catalogs = (
        spark.sql("SHOW CATALOGS")
        .filter(f"catalog = '{catalog_name}'")
        .limit(1)
        .count()
    )

    return catalogs > 0