"""
Asset Definitions - Asset con Extra Metadata

Los Assets pueden incluir metadata adicional para
documentar el recurso y su contenido.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.sdk.definitions.asset import Asset


# Asset con metadata completa
users_asset = Asset(
    uri='s3://my-bucket/data/users.parquet',
    extra={
        'schema': {
            'id': 'int',
            'name': 'string',
            'email': 'string',
            'created_at': 'timestamp'
        },
        'format': 'parquet',
        'compression': 'snappy',
        'owner': 'data-team',
        'refresh_rate': 'daily'
    }
)

orders_asset = Asset(
    uri='postgres://localhost:5432/warehouse/analytics/orders',
    extra={
        'table_schema': 'analytics',
        'table_name': 'orders',
        'row_count_estimate': 1000000,
        'owner': 'sales-team',
        'sla_hours': 4
    }
)


with DAG(
    dag_id='producer_with_metadata',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'metadata']
) as producer:
    
    @task(outlets=[users_asset])
    def extract_users():
        """Extrae users con schema documentado"""
        print("📥 Extracting users...")
        print(f"  - URI: {users_asset.uri}")
        print(f"  - Format: {users_asset.extra['format']}")
        print(f"  - Schema: {users_asset.extra['schema']}")
        
        # Simulación
        users = [
            {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
            {'id': 2, 'name': 'Bob', 'email': 'bob@example.com'}
        ]
        
        print(f"✅ Extracted {len(users)} users")
        return users
    
    @task(outlets=[orders_asset])
    def extract_orders():
        """Extrae orders con metadata"""
        print("📥 Extracting orders...")
        print(f"  - URI: {orders_asset.uri}")
        print(f"  - Owner: {orders_asset.extra['owner']}")
        print(f"  - SLA: {orders_asset.extra['sla_hours']} hours")
        
        orders = [
            {'order_id': 1, 'user_id': 1, 'total': 100},
            {'order_id': 2, 'user_id': 2, 'total': 200}
        ]
        
        print(f"✅ Extracted {len(orders)} orders")
        return orders
    
    extract_users()
    extract_orders()


with DAG(
    dag_id='consumer_with_metadata',
    schedule=[users_asset, orders_asset],
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'metadata']
) as consumer:
    
    @task
    def validate_and_join():
        """Valida schemas y hace join"""
        print("🔍 Validating asset schemas...")
        
        # Acceder metadata de assets
        print(f"Users schema: {users_asset.extra['schema']}")
        print(f"Orders table: {orders_asset.extra['table_name']}")
        
        # Simulación de join
        print("🔗 Joining users and orders...")
        print("✅ Join completed successfully")
    
    validate_and_join()


producer.doc_md = """
# Asset con Metadata

## extra parameter:
```python
Asset(
    uri='s3://bucket/data/users.parquet',
    extra={
        'schema': {...},
        'format': 'parquet',
        'owner': 'team-name',
        'refresh_rate': 'daily'
    }
)
```

## Metadata útil:

### Schema:
```python
'schema': {
    'column1': 'type',
    'column2': 'type'
}
```

### Data quality:
```python
'quality_checks': ['not_null', 'unique'],
'row_count_estimate': 1000000
```

### Ownership:
```python
'owner': 'team-name',
'contact': 'email@example.com'
```

### SLA:
```python
'sla_hours': 4,
'refresh_rate': 'hourly'
```

## Ventajas:
- 📋 Documentación centralizada
- 🔍 Mejor comprensión del asset
- 🤝 Facilita colaboración
- ⚙️ Metadata accesible en código
"""
