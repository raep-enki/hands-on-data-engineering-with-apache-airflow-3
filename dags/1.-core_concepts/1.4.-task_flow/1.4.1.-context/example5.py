"""
TaskFlow - Runtime Configuration con dag_run.conf

Demuestra cómo pasar parámetros al DAG en tiempo de ejecución.
Útil para triggers manuales con configuración dinámica.

dag_run.conf contiene JSON pasado al triggear el DAG.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator


@task
def read_config(**context):
    """Lee configuración pasada al DAG run"""
    dag_run = context['dag_run']
    conf = dag_run.conf or {}  # Vacío si no se pasó config
    
    print("⚙️ Configuración recibida:")
    print(f"  - Completa: {conf}")
    
    # Valores con defaults
    environment = conf.get('environment', 'production')
    region = conf.get('region', 'us-east-1')
    batch_size = conf.get('batch_size', 1000)
    debug_mode = conf.get('debug', False)
    
    print(f"  - Environment: {environment}")
    print(f"  - Region: {region}")
    print(f"  - Batch size: {batch_size}")
    print(f"  - Debug: {debug_mode}")
    
    return {
        'environment': environment,
        'region': region,
        'batch_size': batch_size,
        'debug': debug_mode
    }


@task
def process_with_config(config: dict, logical_date):
    """Procesa datos usando configuración runtime"""
    print(f"🔧 Procesando con config: {config}")
    
    # Lógica basada en environment
    if config['environment'] == 'development':
        print("  🧪 Modo desarrollo: usando datos de prueba")
        data_source = 'test_database'
    elif config['environment'] == 'staging':
        print("  🔍 Modo staging: validación extra")
        data_source = 'staging_database'
    else:
        print("  🏭 Modo producción: datos reales")
        data_source = 'production_database'
    
    # Usar batch_size de config
    batch_size = config['batch_size']
    print(f"  📦 Procesando en batches de {batch_size}")
    
    # Debug mode
    if config['debug']:
        print("  🐛 Debug mode enabled: verbose logging")
    
    # Simular procesamiento
    query = f"""
    SELECT * FROM {data_source}.events
    WHERE date = '{logical_date.strftime('%Y-%m-%d')}'
      AND region = '{config['region']}'
    LIMIT {batch_size}
    """
    
    return {
        'source': data_source,
        'region': config['region'],
        'batch_size': batch_size,
        'query': query,
        'records_processed': batch_size * 0.8  # Simulado
    }


@task
def conditional_task(config: dict):
    """Ejecuta lógica condicional según config"""
    # Feature flags desde config
    enable_ml = config.get('enable_ml', False)
    enable_notifications = config.get('enable_notifications', True)
    
    results = []
    
    if enable_ml:
        print("🤖 ML enabled: running model predictions")
        results.append('ml_predictions')
    else:
        print("🤖 ML disabled: skipping predictions")
    
    if enable_notifications:
        print("📧 Notifications enabled: will send alerts")
        results.append('notifications')
    else:
        print("📧 Notifications disabled")
    
    return {'features_enabled': results}


@task
def validate_config(**context):
    """Valida configuración antes de procesarla"""
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    
    print("✅ Validando configuración...")
    
    errors = []
    
    # Validar environment
    valid_envs = ['development', 'staging', 'production']
    env = conf.get('environment', 'production')
    if env not in valid_envs:
        errors.append(f"Invalid environment: {env}. Must be one of {valid_envs}")
    
    # Validar batch_size
    batch_size = conf.get('batch_size', 1000)
    if not isinstance(batch_size, int) or batch_size <= 0:
        errors.append(f"Invalid batch_size: {batch_size}. Must be positive integer")
    
    # Validar region
    valid_regions = ['us-east-1', 'us-west-2', 'eu-west-1']
    region = conf.get('region', 'us-east-1')
    if region not in valid_regions:
        errors.append(f"Invalid region: {region}. Must be one of {valid_regions}")
    
    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    
    print("✅ Configuration válida")
    return {'status': 'valid', 'config': conf}


with DAG(
    dag_id='taskflow_runtime_config',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'task_flow', 'context']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Pipeline con configuración runtime
    validation = validate_config()
    config = read_config()
    result = process_with_config(config)
    features = conditional_task(config)
    
    end = EmptyOperator(task_id='end')
    
    start >> validation >> config >> [result, features] >> end

dag.doc_md = """
# Runtime Configuration con dag_run.conf

**dag_run.conf**: JSON pasado al DAG cuando se triggerea.

## Cómo pasar configuración:

### CLI:
```bash
# Trigger con config JSON
airflow dags trigger my_dag \\
  --conf '{"environment": "staging", "region": "us-west-2", "batch_size": 5000}'

# O desde archivo
airflow dags trigger my_dag --conf @config.json
```

### API:
```python
import requests

response = requests.post(
    'http://airflow:8080/api/v1/dags/my_dag/dagRuns',
    json={
        'conf': {
            'environment': 'production',
            'region': 'eu-west-1',
            'batch_size': 10000
        }
    },
    auth=('user', 'password')
)
```

### UI:
1. DAGs → my_dag → Trigger DAG
2. "Trigger with config" → JSON editor
3. Ingresar: `{"environment": "development", "region": "us-east-1"}`

## Acceso a configuración:

```python
@task
def my_task(**context):
    dag_run = context['dag_run']
    conf = dag_run.conf or {}  # {} si no hay config
    
    # Leer valores con defaults
    env = conf.get('environment', 'production')
    region = conf.get('region', 'us-east-1')
    
    return {'env': env, 'region': region}
```

## Valores con defaults:

```python
@task
def task_with_defaults(**context):
    conf = context['dag_run'].conf or {}
    
    # Defaults para todos los parámetros
    config = {
        'environment': conf.get('environment', 'production'),
        'region': conf.get('region', 'us-east-1'),
        'batch_size': conf.get('batch_size', 1000),
        'debug': conf.get('debug', False),
        'timeout': conf.get('timeout', 300)
    }
    
    return config
```

## Validación de configuración:

```python
@task
def validate_and_process(**context):
    conf = context['dag_run'].conf or {}
    
    # Validación obligatoria
    required = ['environment', 'region']
    missing = [key for key in required if key not in conf]
    
    if missing:
        raise ValueError(f"Missing required config: {missing}")
    
    # Validación de valores
    valid_envs = ['dev', 'staging', 'prod']
    if conf['environment'] not in valid_envs:
        raise ValueError(f"Invalid environment. Must be one of {valid_envs}")
    
    # Procesamiento
    return process_data(conf)
```

## Casos de uso:

### 1. Environments diferentes
```python
@task
def env_specific_task(**context):
    env = context['dag_run'].conf.get('environment', 'prod')
    
    if env == 'dev':
        db = 'dev_database'
        batch = 100
    elif env == 'staging':
        db = 'staging_database'
        batch = 1000
    else:
        db = 'prod_database'
        batch = 10000
    
    return {'database': db, 'batch_size': batch}

# Trigger:
# airflow dags trigger my_dag --conf '{"environment": "dev"}'
```

### 2. Backfill selectivo
```python
@task
def selective_backfill(**context):
    conf = context['dag_run'].conf or {}
    
    # Backfill solo ciertas tablas
    tables = conf.get('tables', ['all'])
    start_date = conf.get('start_date')
    end_date = conf.get('end_date')
    
    if 'all' in tables:
        tables = get_all_tables()
    
    for table in tables:
        backfill_table(table, start_date, end_date)

# Trigger:
# airflow dags trigger backfill_dag --conf '{
#   "tables": ["users", "orders"],
#   "start_date": "2024-01-01",
#   "end_date": "2024-01-31"
# }'
```

### 3. Feature flags
```python
@task
def feature_flag_task(**context):
    conf = context['dag_run'].conf or {}
    
    # Feature toggles
    enable_ml = conf.get('enable_ml', False)
    enable_cache = conf.get('enable_cache', True)
    enable_notifications = conf.get('notifications', True)
    
    if enable_ml:
        run_ml_model()
    
    if enable_cache:
        use_cache()
    
    if enable_notifications:
        send_notifications()

# Trigger con features:
# airflow dags trigger my_dag --conf '{
#   "enable_ml": true,
#   "enable_cache": false
# }'
```

### 4. Region/datacenter routing
```python
@task
def regional_task(**context):
    region = context['dag_run'].conf.get('region', 'us-east-1')
    
    endpoints = {
        'us-east-1': 'https://api-us-east.example.com',
        'us-west-2': 'https://api-us-west.example.com',
        'eu-west-1': 'https://api-eu.example.com'
    }
    
    api_endpoint = endpoints[region]
    return fetch_data(api_endpoint)

# Trigger para región específica:
# airflow dags trigger my_dag --conf '{"region": "eu-west-1"}'
```

### 5. Batch size tuning
```python
@task
def tunable_batch(**context):
    batch_size = context['dag_run'].conf.get('batch_size', 1000)
    
    # Procesar en batches configurables
    for batch in chunked(get_data(), batch_size):
        process_batch(batch)

# Trigger con batch custom:
# airflow dags trigger my_dag --conf '{"batch_size": 5000}'
```

## Patrón: Config + Params

```python
@task
def combined_config(**context):
    # Runtime config (trigger time)
    conf = context['dag_run'].conf or {}
    
    # DAG params (defined in DAG)
    params = context['params']
    
    # Conf override params
    final_config = {
        **params,  # Defaults from DAG
        **conf     # Runtime overrides
    }
    
    return final_config
```

## Default config pattern:

```python
DEFAULT_CONFIG = {
    'environment': 'production',
    'region': 'us-east-1',
    'batch_size': 1000,
    'timeout': 300,
    'retries': 3
}

@task
def task_with_defaults(**context):
    # Merge runtime config con defaults
    conf = context['dag_run'].conf or {}
    config = {**DEFAULT_CONFIG, **conf}
    
    return process_with_config(config)
```

## Testing con config:

```python
# Test con config específico
from airflow.models import DagRun

def test_my_task():
    # Simular dag_run con config
    mock_context = {
        'dag_run': DagRun(
            conf={'environment': 'test', 'batch_size': 10}
        )
    }
    
    result = my_task.function(**mock_context)
    assert result['environment'] == 'test'
```

## Best practices:

1. **Siempre defaults**: `conf.get('key', default)`
2. **Validar config**: Verificar tipos y valores válidos
3. **Documentar config**: Qué keys acepta el DAG
4. **Seguridad**: No pasar secrets en conf (usar Connections/Variables)
5. **Tipado**: Validar tipos de datos
6. **Logging**: Log config recibido para debugging

## Ejemplo completo:

```python
DEFAULT_CONFIG = {
    'environment': 'production',
    'region': 'us-east-1',
    'batch_size': 1000,
    'enable_ml': False,
    'debug': False
}

@task
def validate_config(**context):
    \"\"\"Valida configuración runtime\"\"\"
    conf = context['dag_run'].conf or {}
    
    # Validaciones
    if 'environment' in conf:
        valid_envs = ['development', 'staging', 'production']
        if conf['environment'] not in valid_envs:
            raise ValueError(f"Invalid environment: {conf['environment']}")
    
    if 'batch_size' in conf:
        if not isinstance(conf['batch_size'], int) or conf['batch_size'] <= 0:
            raise ValueError(f"Invalid batch_size: {conf['batch_size']}")
    
    return {'status': 'valid'}

@task
def load_config(**context):
    \"\"\"Carga configuración con defaults\"\"\"
    conf = context['dag_run'].conf or {}
    config = {**DEFAULT_CONFIG, **conf}
    
    print(f"Final config: {config}")
    return config

@task
def process_data(config: dict, logical_date):
    \"\"\"Procesa datos con configuración\"\"\"
    env = config['environment']
    region = config['region']
    batch_size = config['batch_size']
    
    # Lógica basada en config
    database = f"{env}_database"
    
    query = f\"\"\"
    SELECT * FROM {database}.events
    WHERE date = '{logical_date.strftime('%Y-%m-%d')}'
      AND region = '{region}'
    LIMIT {batch_size}
    \"\"\"
    
    records = execute_query(query)
    
    # ML si enabled
    if config['enable_ml']:
        records = apply_ml_model(records)
    
    return {
        'records_processed': len(records),
        'config_used': config
    }

validation = validate_config()
config = load_config()
result = process_data(config)

validation >> config >> result
```

## Documentación en DAG:

```python
dag.doc_md = \"\"\"
# My DAG

## Runtime Configuration

This DAG accepts the following configuration:

### Required:
- None (all have defaults)

### Optional:
- `environment`: string, one of ['development', 'staging', 'production']
  Default: 'production'
  
- `region`: string, one of ['us-east-1', 'us-west-2', 'eu-west-1']
  Default: 'us-east-1'
  
- `batch_size`: integer > 0
  Default: 1000
  
- `enable_ml`: boolean
  Default: false

### Example:

```bash
airflow dags trigger my_dag --conf '{
  "environment": "staging",
  "region": "eu-west-1",
  "batch_size": 5000,
  "enable_ml": true
}'
```
\"\"\"
```
"""
