from pathlib import Path
import yaml

from pyspark.sql import SparkSession


def load_table_definition(
    table_definition_path: Path,
) -> dict:
    """Load a table definition from YAML."""

    with table_definition_path.open("r") as file:
        return yaml.safe_load(file)


def build_create_table_sql(
    catalog_name: str,
    table_definition: dict,
) -> str:
    """Build CREATE TABLE SQL from a table definition."""

    schema_name = table_definition["schema"]
    table_name = table_definition["name"]
    columns = table_definition["columns"]

    column_definitions = []

    for column in columns:
        column_sql = (
            f"`{column['name']}` {column['type']}"
        )

        if column.get("nullable") is False:
            column_sql += " NOT NULL"

        comment = column.get("comment")
        if comment:
            escaped_comment = comment.replace("'", "''")
            column_sql += f" COMMENT '{escaped_comment}'"

        column_definitions.append(column_sql)

    columns_sql = ",\n".join(column_definitions)

    sql = f"""
    CREATE TABLE IF NOT EXISTS
        `{catalog_name}`.`{schema_name}`.`{table_name}`
    (
        {columns_sql}
    )
    USING DELTA
    """

    comment = table_definition.get("comment")

    if comment:
        escaped_comment = comment.replace("'", "''")

        sql += f"""
        COMMENT '{escaped_comment}'
        """

    return sql


def create_table(
    spark: SparkSession,
    catalog_name: str,
    table_definition: dict,
) -> None:
    """Create one configured table."""

    sql = build_create_table_sql(
        catalog_name=catalog_name,
        table_definition=table_definition,
    )

    spark.sql(sql)


def create_tables_from_directory(
    spark: SparkSession,
    catalog_name: str,
    directory: Path,
) -> None:
    """Create all tables defined in a directory."""

    if not directory.exists():
        return

    for table_file in sorted(directory.glob("*.yml")):
        definition = load_table_definition(table_file)

        create_table(
            spark=spark,
            catalog_name=catalog_name,
            table_definition=definition,
        )

def load_table_definitions(
    directory: Path,
) -> list[dict]:
    definitions = []

    if not directory.exists():
        return definitions

    for table_file in sorted(directory.glob("*.yml")):
        definitions.append(
            load_table_definition(table_file)
        )

    return definitions

