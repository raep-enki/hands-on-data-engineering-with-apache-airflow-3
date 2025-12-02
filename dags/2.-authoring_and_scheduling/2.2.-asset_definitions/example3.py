"""
Asset Definitions - Asset URI Patterns y Convenciones

Los URI de assets deben seguir convenciones para
identificar datos de manera única y consistente.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.sdk.definitions.asset import Asset


# Diferentes tipos de URI patterns

# S3 (más común)
s3_asset = Asset('s3://my-bucket/data/events/year=2024/month=01/data.parquet')

# Local filesystem
local_asset = Asset('file:///opt/airflow/data/processed/users.csv')

# Database table (formato: postgres://host:port/database/schema/table)
postgres_asset = Asset('postgres://localhost:5432/warehouse/public/users')

# API endpoint
api_asset = Asset('https://api.example.com/v1/users')

# Custom schema
custom_asset = Asset('my-datalake://bronze/users/v2')


with DAG(
    dag_id='producer_uri_patterns',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'uri_patterns']
) as producer:
    
    @task(outlets=[s3_asset])
    def write_to_s3(**context):
        """Escribe datos a S3 con particionamiento"""
        logical_date = context['logical_date']
        
        print(f"💾 Writing to S3...")
        print(f"  - Date: {logical_date}")
        print(f"  - URI: {s3_asset.uri}")
        print("✅ Data written to S3")
    
    @task(outlets=[postgres_asset])
    def write_to_postgres():
        """Escribe a tabla PostgreSQL"""
        print(f"💾 Writing to PostgreSQL...")
        print(f"  - URI: {postgres_asset.uri}")
        print("✅ Table updated")
    
    @task(outlets=[custom_asset])
    def write_to_datalake():
        """Escribe a custom data lake"""
        print(f"💾 Writing to data lake...")
        print(f"  - URI: {custom_asset.uri}")
        print("✅ Data lake updated")
    
    write_to_s3()
    write_to_postgres()
    write_to_datalake()


with DAG(
    dag_id='consumer_uri_patterns',
    schedule=[s3_asset, postgres_asset],
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'asset_definitions', 'uri_patterns']
) as consumer:
    
    @task
    def process_data():
        """Procesa datos de múltiples sources"""
        print("⚙️ Processing data from multiple sources...")
        print(f"  - S3: {s3_asset.uri}")
        print(f"  - PostgreSQL: {postgres_asset.uri}")
        print("✅ Processing complete")
    
    process_data()


producer.doc_md = """
# Asset URI Patterns

## Convenciones:

### S3/GCS:
```python
Asset('s3://bucket/path/file.ext')
Asset('s3://bucket/year=2024/month=01/data.parquet')
Asset('gcs://bucket/data/users.json')
```

### Local files:
```python
Asset('file:///absolute/path/to/file.csv')
```

### Databases:
```python
Asset('postgres://host:port/database/schema/table')
Asset('mysql://host:port/database/table')
Asset('snowflake://account/database/schema/table')
```

### HTTP/API:
```python
Asset('https://api.example.com/endpoint')
```

### Custom:
```python
Asset('my-system://identifier')
Asset('datalake://layer/domain/entity')
```

## Best Practices:

1. **Unique**: URI debe identificar datos únicos
2. **Descriptive**: Nombre claro del recurso
3. **Versioned**: Incluir versión si aplica
4. **Consistent**: Usar mismo formato en toda organización

## Anti-patterns:
- ❌ `Asset('users')` - muy genérico
- ❌ `Asset('data')` - no identifica recurso
- ✅ `Asset('s3://bucket/domain/users/v1')`
"""
