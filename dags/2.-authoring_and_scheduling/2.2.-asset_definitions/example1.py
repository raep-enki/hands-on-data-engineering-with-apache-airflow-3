"""
Asset Definitions - Introducción a Dataset Scheduling

Los Assets (Datasets) permiten dependencias basadas en datos
entre DAGs. Un DAG se activa cuando otro produce un dataset.

Introducido en Airflow 2.4+, reemplaza ExternalTaskSensor.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.sdk.definitions.asset import Asset


# Definir asset (URI identifica el dato)
user_data_asset = Asset('s3://my-bucket/data/users.parquet')


# DAG Producer: produce el asset
with DAG(
    dag_id='producer_basic_asset',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'producer']
) as producer_dag:
    
    @task(outlets=[user_data_asset])  # Declara que produce el asset
    def extract_users():
        """Extrae datos de usuarios y actualiza asset"""
        print("📥 Extracting users from database...")
        
        users = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
            {'id': 3, 'name': 'Charlie'}
        ]
        
        print(f"✅ Extracted {len(users)} users")
        print(f"📦 Asset updated: {user_data_asset.uri}")
        
        return users
    
    extract_users()


# DAG Consumer: se activa cuando el asset es actualizado
with DAG(
    dag_id='consumer_basic_asset',
    schedule=[user_data_asset],  # Triggered por el asset
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'consumer']
) as consumer_dag:
    
    @task
    def process_users():
        """Procesa users cuando asset está disponible"""
        print("⚙️ Processing users from asset...")
        print(f"📦 Asset URI: {user_data_asset.uri}")
        
        # Aquí leerías el asset real (S3, database, etc.)
        print("✅ Users processed successfully")
    
    process_users()


producer_dag.doc_md = """
# Producer DAG

Produce un asset cuando completa.

## outlets parameter:
- Declara qué assets produce esta task
- Puede ser lista de múltiples assets
- Asset se actualiza al completar task

## URI patterns:
- `s3://bucket/path/file.ext`
- `file:///local/path`
- `postgres://db/table`
- Custom: `my-system://identifier`
"""

consumer_dag.doc_md = """
# Consumer DAG

Se activa automáticamente cuando el asset es producido.

## schedule=[asset]:
- Reemplaza schedule con lista de assets
- DAG se ejecuta cuando asset es actualizado
- Puede esperar múltiples assets: `[asset1, asset2]`

## Ventajas vs ExternalTaskSensor:
- ✅ No polling (más eficiente)
- ✅ Mejor UI (lineage graph)
- ✅ Multiple producers/consumers
- ✅ Declarativo (no código sensor)

## Data-aware scheduling:
- Schedule basado en datos, no tiempo
- DAG se ejecuta cuando datos están listos
"""
