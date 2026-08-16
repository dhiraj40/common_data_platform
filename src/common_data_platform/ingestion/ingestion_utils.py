from pathlib import Path
from uuid import uuid4

import yaml


def load_ingestion_config(
    config_path: Path,
) -> dict:
    """Load ingestion configuration from YAML."""

    if not config_path.exists():
        raise FileNotFoundError(
            f"Ingestion configuration not found: {config_path}"
        )

    with config_path.open("r") as file:
        return yaml.safe_load(file)


def load_ingestion_configs(
    directory: Path,
) -> list[dict]:
    """Load all ingestion configurations from a directory."""

    if not directory.exists():
        raise FileNotFoundError(
            f"Ingestion configuration directory not found: {directory}"
        )

    return [
        load_ingestion_config(config_file)
        for config_file in sorted(directory.glob("*.yml"))
    ]

def build_checkpoint_path(
    catalog_name: str,
    layer: str,
    dataset_name: str,
) -> str:
    """Build dataset-specific Auto Loader checkpoint root path."""

    return (
        f"/Volumes/{catalog_name}/"
        f"platform/checkpoints/"
        f"{layer}/{dataset_name}"
    )

def generate_batch_id() -> str:
    """Generate a unique ingestion batch identifier."""

    return str(uuid4())


def build_volume_path(
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    relative_path: str | None = None,
) -> str:
    """Build a Unity Catalog Volume path."""

    path = (
        f"/Volumes/{catalog_name}/"
        f"{schema_name}/{volume_name}"
    )

    if relative_path:
        path = f"{path}/{relative_path.strip('/')}"

    return path


def build_table_name(
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> str:
    """Build fully-qualified Unity Catalog table name."""

    return (
        f"`{catalog_name}`."
        f"`{schema_name}`."
        f"`{table_name}`"
    )