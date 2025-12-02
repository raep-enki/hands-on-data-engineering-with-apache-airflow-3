"""
Asset Definitions - Múltiples Assets y Dependencias

Un DAG puede producir múltiples assets y un consumer
puede esperar múltiples assets simultáneamente.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.sdk.definitions.asset import Asset


# Definir múltiples assets
users_asset = Asset('s3://bucket/data/users.parquet')
orders_asset = Asset('s3://bucket/data/orders.parquet')
products_asset = Asset('s3://bucket/data/products.parquet')


# Producer 1: extrae users y orders
with DAG(
    dag_id='producer_users_orders',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'asset_definitions']
) as producer1:
    
    @task(outlets=[users_asset])
    def extract_users():
        print("📥 Extracting users...")
        print(f"✅ Asset updated: {users_asset.uri}")
        return {'count': 1000}
    
    @task(outlets=[orders_asset])
    def extract_orders():
        print("📥 Extracting orders...")
        print(f"✅ Asset updated: {orders_asset.uri}")
        return {'count': 5000}
    
    extract_users()
    extract_orders()


# Producer 2: extrae products
with DAG(
    dag_id='producer_products',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'asset_definitions']
) as producer2:
    
    @task(outlets=[products_asset])
    def extract_products():
        print("📥 Extracting products...")
        print(f"✅ Asset updated: {products_asset.uri}")
        return {'count': 500}
    
    extract_products()


# Consumer: espera TODOS los assets (AND logic)
with DAG(
    dag_id='consumer_all_assets',
    schedule=[users_asset, orders_asset, products_asset],  # Espera los 3
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'asset_definitions']
) as consumer:
    
    @task
    def create_report():
        """Se ejecuta cuando los 3 assets están listos"""
        print("📊 Creating report with all data...")
        print(f"  - Users: {users_asset.uri}")
        print(f"  - Orders: {orders_asset.uri}")
        print(f"  - Products: {products_asset.uri}")
        print("✅ Report completed")
    
    create_report()


consumer.doc_md = """
# Consumer con Múltiples Assets

## AND logic (default):
```python
schedule=[asset1, asset2, asset3]
```
DAG se ejecuta cuando **TODOS** los assets están listos.

## Trigger behavior:
1. producer_users_orders completa → users_asset y orders_asset actualizados
2. producer_products completa → products_asset actualizado
3. Ahora los 3 assets están listos → consumer se ejecuta

## Timeline:
```
T0: producer_users_orders completa
    → users_asset ✅, orders_asset ✅, products_asset ❌
    
T1: producer_products completa
    → users_asset ✅, orders_asset ✅, products_asset ✅
    → consumer se ejecuta automáticamente
```

## Use cases:
- Esperar múltiples fuentes para join
- Pipeline con dependencias de múltiples ETLs
- Reporting que necesita todos los datos
"""
