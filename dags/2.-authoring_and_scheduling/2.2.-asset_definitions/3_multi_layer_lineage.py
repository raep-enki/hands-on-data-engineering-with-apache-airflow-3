"""
Asset Definitions - Cadenas de Assets (Lineage)

Los assets pueden formar cadenas complejas donde
un consumer es también producer de otro asset.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.sdk.definitions.asset import Asset


# Definir assets en diferentes capas
raw_data_asset = Asset('s3://bucket/raw/transactions.csv')
cleaned_data_asset = Asset('s3://bucket/cleaned/transactions.parquet')
aggregated_asset = Asset('s3://bucket/aggregated/daily_metrics.parquet')
report_asset = Asset('s3://bucket/reports/dashboard_data.json')


# Layer 1: Raw data extraction
with DAG(
    dag_id='layer1_extract_raw',
    schedule='@hourly',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'lineage']
) as layer1:
    
    @task(outlets=[raw_data_asset])
    def extract_raw_transactions():
        print("📥 Extracting raw transactions...")
        print(f"✅ Asset: {raw_data_asset.uri}")
        return {'count': 1000}
    
    extract_raw_transactions()


# Layer 2: Clean data (consumer de raw, producer de cleaned)
with DAG(
    dag_id='layer2_clean_data',
    schedule=[raw_data_asset],  # Triggered por raw
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'lineage']
) as layer2:
    
    @task(outlets=[cleaned_data_asset])
    def clean_transactions():
        print("🧹 Cleaning transactions...")
        print(f"  - Input: {raw_data_asset.uri}")
        print(f"  - Output: {cleaned_data_asset.uri}")
        print("✅ Data cleaned")
        return {'count': 950}
    
    clean_transactions()


# Layer 3: Aggregate data (consumer de cleaned, producer de aggregated)
with DAG(
    dag_id='layer3_aggregate',
    schedule=[cleaned_data_asset],  # Triggered por cleaned
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'lineage']
) as layer3:
    
    @task(outlets=[aggregated_asset])
    def aggregate_metrics():
        print("📊 Aggregating metrics...")
        print(f"  - Input: {cleaned_data_asset.uri}")
        print(f"  - Output: {aggregated_asset.uri}")
        print("✅ Metrics aggregated")
        return {'daily_total': 50000}
    
    aggregate_metrics()


# Layer 4: Generate report (consumer de aggregated, producer de report)
with DAG(
    dag_id='layer4_report',
    schedule=[aggregated_asset],  # Triggered por aggregated
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'lineage']
) as layer4:
    
    @task(outlets=[report_asset])
    def generate_dashboard():
        print("📈 Generating dashboard data...")
        print(f"  - Input: {aggregated_asset.uri}")
        print(f"  - Output: {report_asset.uri}")
        print("✅ Dashboard ready")
    
    generate_dashboard()


layer1.doc_md = """
# Asset Lineage - Layer 1 (Extract)

Primer paso: extracción de datos raw.

## Flow:
```
layer1_extract_raw (schedule: @hourly)
  ↓ produces
raw_data_asset
  ↓ triggers
layer2_clean_data
```
"""

layer2.doc_md = """
# Asset Lineage - Layer 2 (Clean)

Consumer de raw, producer de cleaned.

## Flow:
```
raw_data_asset (from layer1)
  ↓ triggers
layer2_clean_data
  ↓ produces
cleaned_data_asset
  ↓ triggers
layer3_aggregate
```

Este DAG es:
- **Consumer** de raw_data_asset
- **Producer** de cleaned_data_asset
"""

layer3.doc_md = """
# Asset Lineage - Layer 3 (Aggregate)

Pipeline completo de 4 capas:

```
Layer 1: Extract    → raw_data_asset
Layer 2: Clean      → cleaned_data_asset
Layer 3: Aggregate  → aggregated_asset
Layer 4: Report     → report_asset
```

## Timeline:
```
T0: layer1 (hourly) completa
    → raw_data_asset updated
    
T1: layer2 triggered automáticamente
    → cleaned_data_asset updated
    
T2: layer3 triggered automáticamente
    → aggregated_asset updated
    
T3: layer4 triggered automáticamente
    → report_asset updated
```

## Ventajas:
- ✅ Desacoplamiento de DAGs
- ✅ Ejecución basada en disponibilidad de datos
- ✅ Lineage visible en UI
- ✅ Retry independiente por layer
"""
