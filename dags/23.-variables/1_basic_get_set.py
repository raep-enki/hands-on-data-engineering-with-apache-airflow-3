"""
Variables - Introducción a Airflow Variables

Variables permiten almacenar configuración global
que puede ser accedida desde cualquier DAG.

Útil para configuración que cambia sin modificar código.
"""

import datetime

from airflow.sdk import DAG, task, Variable


@task
def read_simple_variable():
    """Lee una variable simple"""
    print("📖 Leyendo variable simple...")
    
    # Leer variable (con default si no existe)
    api_key = Variable.get("api_key", default="default_key")
    environment = Variable.get("environment", default="production")
    
    print(f"  - API Key: {api_key[:5]}... (truncated)")
    print(f"  - Environment: {environment}")
    
    return {'api_key': api_key, 'environment': environment}


@task
def read_json_variable():
    """Lee variable JSON deserializada"""
    print("📖 Leyendo variable JSON...")
    
    # Variable como JSON (deserialize_json=True)
    config = Variable.get("app_config", default='{}', deserialize_json=True)
    
    # Si no existe, retorna {}
    print(f"  - Config: {config}")
    
    # Acceder a valores
    db_host = config.get('database', {}).get('host', 'localhost')
    db_port = config.get('database', {}).get('port', 5432)
    
    print(f"  - DB Host: {db_host}")
    print(f"  - DB Port: {db_port}")
    
    return {'db_host': db_host, 'db_port': db_port}


@task
def use_variables_in_logic(config: dict):
    """Usa variables en lógica de negocio"""
    environment = Variable.get("environment", default="production")
    
    print(f"🔧 Ejecutando en environment: {environment}")
    
    if environment == "development":
        print("  - Usando datos de prueba")
        data_source = "test_db"
        batch_size = 100
    elif environment == "staging":
        print("  - Usando datos de staging")
        data_source = "staging_db"
        batch_size = 1000
    else:
        print("  - Usando datos de producción")
        data_source = "prod_db"
        batch_size = 10000
    
    return {
        'environment': environment,
        'data_source': data_source,
        'batch_size': batch_size
    }


with DAG(
    dag_id='variables_basic_usage',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'variables']
) as dag:
    
    simple = read_simple_variable()
    json_var = read_json_variable()
    logic = use_variables_in_logic(simple)

dag.doc_md = """
# Airflow Variables

Variables almacenan configuración global accesible desde cualquier DAG.

## Crear variables:

### UI:
Admin → Variables → + → Add Variable
- Key: api_key
- Value: abc123xyz

### CLI:
```bash
# Simple
airflow variables set api_key abc123xyz

# JSON
airflow variables set app_config '{"db": {"host": "localhost"}}'

# Desde archivo
airflow variables import variables.json
```

### Python:
```python
from airflow.models import Variable
Variable.set("api_key", "abc123")
Variable.set("config", {"key": "value"}, serialize_json=True)
```

## Leer variables:

```python
from airflow.models import Variable

# Simple
value = Variable.get("api_key")

# Con default
value = Variable.get("api_key", default="default")

# JSON
config = Variable.get("config", deserialize_json=True)
```

## Cuándo usar Variables:
- Configuración que cambia sin deploy
- API keys (mejor usar Connections para secrets)
- Environment flags (dev/staging/prod)
- Feature flags
- Configuración global

## Variables vs Params:
- **Variables**: Global, todos los DAGs, persiste
- **Params**: Por DAG, runtime, no persiste
"""
