# Common Data Platform

A Databricks-based Common Data Platform for ingesting, standardizing, validating, and publishing reusable supply-chain data.

The platform owns data through the **Trusted** layer. Product-specific logic such as lead-time calculations, forecasting, analytics, RAG, and agents is built downstream in product-owned schemas.

## Architecture

```text
Source Systems / Files
        |
        v
      Raw
        |
        v
   Foundation
        |
        v
     Trusted
        |
        +-------------------------------+
        |               |               |
        v               v               v
   AI / Agents      Forecasting      Analytics
   Product Layer    Product Layer    Product Layer
```

### Raw

Preserves source data with minimal modification and adds platform ingestion metadata.

- Land source files in Unity Catalog Volumes.
- Ingest files using Databricks Auto Loader.
- Preserve source-level values.
- Add ingestion metadata.
- Maintain Auto Loader checkpoints outside Raw.

### Foundation

Standardizes source-oriented records into consistent technical contracts.

- Normalize identifiers, casing, and text values.
- Handle invalid/null keys.
- Deduplicate source records.
- Preserve source identity and source lifecycle timestamps.
- Apply table-specific processing strategies.
- Merge standardized records into Foundation tables.

Foundation processing is intentionally **table-specific**.

### Trusted

Publishes certified and reusable business entities and facts.

- Apply business-level validation.
- Validate relationships between reusable entities.
- Remove source-specific technical identifiers not required downstream.
- Provide stable contracts for products.

Trusted does **not** contain product-specific calculations such as lead time, supplier scores, forecasts, RAG chunks, embeddings, or agent evaluation data.

## Environments

```text
common_data_platform_dev
common_data_platform_uat
common_data_platform_prod
```

Each catalog contains:

```text
raw
foundation
trusted
platform
```

Environment configuration lives under:

```text
resources/config/dev.yml
resources/config/uat.yml
resources/config/prod.yml
```

## Unity Catalog Layout

```text
common_data_platform_dev
|
+-- raw
|   +-- material
|   +-- plant
|   +-- supplier
|   +-- purchase_order
|   +-- shipment
|   +-- inventory
|   +-- Volumes
|       +-- source_files
|       +-- documents
|
+-- foundation
|   +-- material
|   +-- plant
|   +-- supplier
|   +-- purchase_order
|   +-- shipment
|   +-- inventory
|
+-- trusted
|   +-- material
|   +-- plant
|   +-- supplier
|   +-- purchase_order
|   +-- shipment
|   +-- inventory
|
+-- platform
    +-- Volumes
        +-- checkpoints
```

Checkpoint example:

```text
/Volumes/common_data_platform_dev/platform/checkpoints/raw/material/
```

## Data Model

| Dataset | Type | Description |
|---|---|---|
| Material | Master | Material/product master data |
| Plant | Master | Plant/location master data |
| Supplier | Master | Supplier master data |
| Purchase Order | Transactional | Purchase-order line records |
| Shipment | Transactional | Physical shipment records |
| Inventory | Snapshot | Material inventory snapshots |

### Business keys

```text
Material:
source_system + material_id

Plant:
source_system + plant_id

Supplier:
source_system + supplier_id

Purchase Order:
source_system + purchase_order_id + purchase_order_line

Shipment:
source_system + shipment_id

Inventory:
source_system + inventory_snapshot_date + material_id + plant_id + storage_location
```

A PO line can have multiple shipments. Inventory preserves historical snapshots.

## Timestamp Convention

Business timestamps remain table-specific, for example:

```text
order_date
requested_delivery_date
shipment_date
actual_delivery_date
inventory_snapshot_date
```

Source record lifecycle timestamps:

```text
record_created_timestamp
record_updated_timestamp
```

These are preserved through Foundation and Trusted.

Raw also contains platform ingestion metadata:

```text
ingestion_timestamp
ingestion_batch_id
source_file
source_system
```

`ingestion_timestamp` represents when the platform ingested the Raw record and is different from business-event and source-update timestamps.

## Raw Ingestion

Source files land under:

```text
/Volumes/<catalog>/raw/source_files/<dataset>/
```

Auto Loader checkpoints are stored under:

```text
/Volumes/<catalog>/platform/checkpoints/raw/<dataset>/
```

Current ingestion patterns:

```text
Material / Plant / Supplier
    -> scheduled execution
    -> Auto Loader AvailableNow

Purchase Order / Inventory
    -> scheduled execution
    -> Auto Loader AvailableNow

Shipment
    -> file-arrival workflow trigger
    -> Auto Loader AvailableNow
```

## Foundation Processing

Foundation logic is implemented in notebooks for step-by-step debugging.

### Material / Plant / Supplier

```text
Read Raw history
    -> standardize
    -> validate key
    -> keep latest source record
    -> MERGE into Foundation
```

### Purchase Order

Keeps the latest version for:

```text
source_system + purchase_order_id + purchase_order_line
```

### Shipment

Keeps the latest version for:

```text
source_system + shipment_id
```

### Inventory

Preserves different snapshot dates while resolving duplicate/corrected versions of the same snapshot key before MERGE.

## Trusted Processing

### Master entities

Trusted publishes certified:

```text
material
plant
supplier
```

### Purchase Order

A Trusted PO is validated against:

```text
Trusted Material
Trusted Supplier
Trusted Plant
```

### Shipment

A Trusted shipment validates:

```text
Material
Supplier
Plant
```

If a PO reference is supplied, that PO reference is also validated.

### Inventory

Trusted Inventory validates:

```text
Material
Plant
```

while preserving valid historical snapshots.

## Project Structure

```text
Common Data Platform/
|
+-- src/
|   +-- common_data_platform/
|       +-- __init__.py
|       +-- setup/
|       |   +-- __init__.py
|       |   +-- config.py
|       |   +-- catalog.py
|       |   +-- schemas.py
|       |   +-- volumes.py
|       |   +-- tables.py
|       |   +-- validation.py
|       |   +-- product_provisioning.py
|       +-- ingestion/
|       |   +-- __init__.py
|       |   +-- ingestion_utils.py
|       |   +-- file_ingestion.py
|       |   +-- raw_ingestion.py
|       +-- foundation/
|       |   +-- __init__.py
|       |   +-- transformations.py
|       |   +-- merge.py
|       +-- trusted/
|       |   +-- __init__.py
|       |   +-- transformations.py
|       |   +-- merge.py
|       +-- quality/
|       +-- utils/
|
+-- notebooks/
|   +-- setup/
|   +-- ingestion/
|   +-- foundation/
|   |   +-- material.ipynb
|   |   +-- plant.ipynb
|   |   +-- supplier.ipynb
|   |   +-- purchase_order.ipynb
|   |   +-- shipment.ipynb
|   |   +-- inventory.ipynb
|   +-- trusted/
|       +-- material.ipynb
|       +-- plant.ipynb
|       +-- supplier.ipynb
|       +-- purchase_order.ipynb
|       +-- shipment.ipynb
|       +-- inventory.ipynb
|
+-- resources/
|   +-- config/
|   +-- ingestion/
|   +-- tables/
|   |   +-- raw/
|   |   +-- foundation/
|   |   +-- trusted/
|   +-- workflows/
|       +-- platform_setup.yml
|       +-- raw_daily.yml
|       +-- raw_4x_daily.yml
|       +-- raw_shipment_file_arrival.yml
|       +-- foundation.yml
|       +-- trusted.yml
|       +-- end_to_end.yml
|
+-- tests/
+-- databricks.yml
+-- pyproject.toml
+-- requirements.txt
+-- README.md
```

Some framework modules may remain placeholders until reusable logic is required.

## Platform Setup

The setup notebook provisions:

1. Catalog
2. Schemas
3. Volumes
4. Raw tables
5. Foundation tables
6. Trusted tables
7. Platform validation

Run:

```text
notebooks/setup/01_platform_setup.ipynb
```

with an environment such as:

```text
dev
```

## Databricks Bundles

Validate:

```bash
databricks bundle validate -t dev
```

Deploy:

```bash
databricks bundle deploy -t dev
```

Run Foundation:

```bash
databricks bundle run -t dev foundation_job
```

Run Trusted:

```bash
databricks bundle run -t dev trusted_job
```

The same pattern applies to `uat` and `prod`.

## Workflow Dependencies

### Foundation

Foundation datasets are independent and can run in parallel.

```text
              +-- material
              +-- plant
Raw ----------+-- supplier
              +-- purchase_order
              +-- shipment
              +-- inventory
```

### Trusted

```text
material -----+
plant --------+---- purchase_order ---- shipment
supplier -----+

material -----+
              +---- inventory
plant --------+
```

## Configuration-Driven Design

```text
resources/config/       -> environment configuration
resources/ingestion/    -> ingestion configuration
resources/tables/       -> table contracts
resources/workflows/    -> workflow definitions
```

Implementation should remain environment-independent wherever possible.

## Design Principles

1. Raw preserves source truth.
2. Foundation standardizes source-oriented records.
3. Trusted publishes certified reusable business data.
4. Product-specific logic stays outside the Common Data Platform.
5. Processing strategy is selected per dataset.
6. Business timestamps, source lifecycle timestamps, and platform ingestion timestamps are separate concepts.
7. Physical/business entities are normally retained; source deletion/status flags are preferred over physical deletion when provided.
8. Operational state such as Auto Loader checkpoints belongs in the Platform schema.
9. Configuration controls environment differences.
10. Notebook transformations remain visible and stepwise for easier debugging.

## Current Status

Implemented:

- Environment configuration
- Catalog/schema/volume provisioning
- YAML-driven table provisioning
- Raw Auto Loader ingestion
- Raw ingestion workflows
- Foundation transformations for all six datasets
- Trusted transformations for all six datasets
- Foundation workflow
- Trusted workflow
- Databricks bundle deployment

Planned improvements:

- Automated table schema evolution from YAML
- Data-quality framework and rejected-record handling
- Unit and integration tests
- End-to-end workflow orchestration
- Package the Python library as a wheel
- Remove temporary notebook `sys.path` setup
- Product provisioning automation
- Monitoring, audit, and operational dashboards

## Downstream Products

The Common Data Platform intentionally stops at Trusted.

Examples:

```text
Supply Chain Agent
Forecasting Platform
Supply Chain Analytics
Lead-Time Product
Supplier Performance Product
```

Each product can use Trusted as its certified input and own its product-specific schema, tools, models, indexes, and application logic.
