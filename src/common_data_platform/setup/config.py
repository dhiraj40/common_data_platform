from pathlib import Path
import yaml

def load_config(environment: str) -> dict:
    """
    Load environment-specific platform configuration.

    Supported environments:
    dev, uat, prod
    """

    environment = environment.lower()

    if environment not in {"dev", "uat", "prod"}:
        raise ValueError(
            f"Unsupported environment: {environment}. "
            "Expected one of: dev, uat, prod."
        )

    project_root = Path(__file__).resolve().parents[3]

    config_path = (
        project_root
        / "resources"
        / "config"
        / f"{environment}.yml"
    )

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}"
        )

    with config_path.open("r") as file:
        config = yaml.safe_load(file)

    base_catalog = config["base_catalog"]
    catalog_suffix = config["catalog_suffix"]

    config["environment"] = environment
    config["catalog_name"] = f"{base_catalog}{catalog_suffix}"

    return config