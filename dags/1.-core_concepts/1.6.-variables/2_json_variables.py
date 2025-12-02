"""
Variables - JSON Variables para Configuración Compleja

Variables JSON permiten almacenar configuración estructurada
que puede ser accedida y deserializada automáticamente.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.models import Variable


@task
def setup_example_variables():
    """Setup de variables de ejemplo (solo para demo)"""
    print("🔧 Configurando variables de ejemplo...")
    
    # En producción, esto se hace via UI o CLI
    database_config = {
        "host": "db.example.com",
        "port": 5432,
        "database": "analytics",
        "pool_size": 10
    }
    
    api_config = {
        "base_url": "https://api.example.com",
        "timeout": 30,
        "retries": 3,
        "endpoints": {
            "users": "/v1/users",
            "orders": "/v1/orders",
            "products": "/v1/products"
        }
    }
    
    # Set variables (serialize_json=True)
    Variable.set("database_config", database_config, serialize_json=True)
    Variable.set("api_config", api_config, serialize_json=True)
    
    print("✅ Variables configuradas")
    return {"status": "configured"}


@task
def use_database_config():
    """Usa configuración de base de datos"""
    print("🗄️ Leyendo config de DB...")
    
    # Leer y deserializar JSON
    db_config = Variable.get("database_config", 
                             default_var='{"host": "localhost"}',
                             deserialize_json=True)
    
    host = db_config.get('host')
    port = db_config.get('port', 5432)
    database = db_config.get('database', 'default')
    
    connection_string = f"postgresql://{host}:{port}/{database}"
    
    print(f"  - Connection: {connection_string}")
    return {'connection': connection_string}


@task
def use_api_config():
    """Usa configuración de API"""
    print("🌐 Leyendo config de API...")
    
    api_config = Variable.get("api_config",
                              default_var='{}',
                              deserialize_json=True)
    
    base_url = api_config.get('base_url', 'https://api.default.com')
    endpoints = api_config.get('endpoints', {})
    timeout = api_config.get('timeout', 30)
    
    users_url = base_url + endpoints.get('users', '/users')
    orders_url = base_url + endpoints.get('orders', '/orders')
    
    print(f"  - Users endpoint: {users_url}")
    print(f"  - Orders endpoint: {orders_url}")
    print(f"  - Timeout: {timeout}s")
    
    return {
        'users_url': users_url,
        'orders_url': orders_url,
        'timeout': timeout
    }


with DAG(
    dag_id='variables_json_config',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'variables']
) as dag:
    
    setup = setup_example_variables()
    db = use_database_config()
    api = use_api_config()
    
    setup >> [db, api]

dag.doc_md = """
# JSON Variables

## Crear JSON variable:

### CLI:
```bash
airflow variables set database_config '{
  "host": "db.example.com",
  "port": 5432,
  "database": "analytics"
}'
```

### UI:
Admin → Variables → +
- Key: database_config
- Value: {"host": "db.example.com", "port": 5432}

### Python:
```python
config = {"host": "db.example.com", "port": 5432}
Variable.set("database_config", config, serialize_json=True)
```

## Leer JSON variable:
```python
config = Variable.get("database_config", deserialize_json=True)
host = config.get('host')
port = config.get('port', 5432)  # Con default
```

## Ventajas:
- Configuración estructurada
- Múltiples valores en una variable
- Fácil actualización
- Type-safe con defaults
"""
