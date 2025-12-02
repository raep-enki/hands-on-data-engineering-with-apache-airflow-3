"""
Challenge: Pipeline Que Se Adapta a Cada Ejecución

Tienes un pipeline ETL que debe comportarse diferente según cómo lo ejecutes. A veces quieres
procesar en modo "development" (rápido, sample de datos), a veces en "production" (completo,
validaciones exhaustivas). A veces extraes de API, a veces de S3, a veces de base de datos.
A veces procesas 100 registros, a veces 10,000.

Todas esas decisiones deben tomarse en **runtime** cuando "triggeas" el DAG manualmente desde la UI,
no hardcodeadas en el código. Es como una función con parámetros: mismo código, comportamiento
diferente según los inputs.

Además, necesitas acceder a **información del contexto de Airflow**: ¿qué fecha se está procesando?
¿cuál es el intervalo de tiempo? ¿en qué run estamos? Son datos que Airflow te da automáticamente
en el `context` de cada tarea.

**Este es el PRIMER challenge que usa TaskFlow API (@task)**, que simplifica trabajar con context.

**El flujo con contexto dinámico:**

**Task 1: Leer configuración runtime**
```python
@task(task_id='read_runtime_config')
def read_config(**context):
    # Leer parámetros pasados al triggear el DAG
    dag_run = context['dag_run']
    conf = dag_run.conf or {}  # dag_run.conf contiene params del trigger
    
    env = conf.get('environment', 'development')  # default: dev
    source = conf.get('data_source', 's3')        # default: s3
    limit = conf.get('record_limit', 100)         # default: 100
    
    print(f"Running in {env} mode, source: {source}, limit: {limit}")
    return {'env': env, 'source': source, 'limit': limit}
```

**Task 2: Acceder a fechas del context**
```python
@task(task_id='process_date_range')
def process_dates(**context):
    # Airflow provee automáticamente estas variables
    logical_date = context['logical_date']  # Fecha lógica del run
    data_interval_start = context['data_interval_start']  # Inicio del intervalo
    data_interval_end = context['data_interval_end']      # Fin del intervalo
    
    print(f"Processing data from {data_interval_start} to {data_interval_end}")
    print(f"Logical date: {logical_date}")
    
    # Formatear para queries
    start_str = data_interval_start.strftime('%Y-%m-%d %H:%M:%S')
    end_str = data_interval_end.strftime('%Y-%m-%d %H:%M:%S')
    
    return {'start': start_str, 'end': end_str}
```

**Task 3: Acceder a metadata del run**
```python
@task(task_id='get_run_metadata')
def get_metadata(**context):
    # Metadata del DAG run
    run_id = context['run_id']              # ID único del run
    dag_id = context['dag'].dag_id          # Nombre del DAG
    task_id = context['task_instance'].task_id  # Nombre de la tarea
    
    print(f"DAG: {dag_id}, Run: {run_id}, Task: {task_id}")
    
    # Info útil para logging y debugging
    return {'run_id': run_id, 'dag_id': dag_id, 'task_id': task_id}
```

**Task 4: Extracción adaptativa**
```python
@task(task_id='extract_data_adaptive')
def extract_data(config, **context):
    # Usa la config runtime para decidir de dónde extraer
    source = config['source']
    limit = config['limit']
    env = config['env']
    
    if source == 's3':
        print(f"Extracting from S3 with limit {limit}")
        query = f"SELECT * FROM s3_bucket LIMIT {limit}"
    elif source == 'api':
        print(f"Extracting from API with limit {limit}")
        query = f"GET /api/data?limit={limit}"
    elif source == 'database':
        print(f"Extracting from database with limit {limit}")
        query = f"SELECT * FROM transactions LIMIT {limit}"
    
    # En dev, usa sample. En prod, usa todo
    if env == 'development':
        print("DEV MODE: Using sample data for speed")
    else:
        print("PROD MODE: Full validation and processing")
    
    return {'query': query, 'row_count': limit}
```

**Task 5: Transformación condicional**
```python
@task(task_id='transform_conditional')
def transform_data(config, extract_result, date_range, **context):
    env = config['env']
    row_count = extract_result['row_count']
    
    print(f"Transforming {row_count} rows for period {date_range['start']} - {date_range['end']}")
    
    if env == 'production':
        # Validaciones exhaustivas en prod
        print("Running: schema validation, null checks, duplicate detection, outlier removal")
    else:
        # Validaciones mínimas en dev
        print("Running: basic schema validation only")
    
    return {'transformed_rows': row_count, 'quality_score': 0.95}
```

**Task 6: Reporte con contexto completo**
```python
@task(task_id='generate_context_report')
def generate_report(config, metadata, transform_result, **context):
    # Combina toda la info del contexto en un reporte
    report = f""""""
    print(report)
    return report
```

**Flujo completo con TaskFlow API:**

```python
with DAG(
    dag_id='context_challenge',
    schedule='@hourly',
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=['challenge', 'context', 'taskflow']
) as dag:
    config = read_config()
    dates = process_dates()
    metadata = get_metadata()
    
    extracted = extract_data(config)
    transformed = transform_data(config, extracted, dates)
    report = generate_report(config, metadata, transformed)
```

**Cómo triggear con parámetros desde la UI:**

Cuando triggeas manualmente desde Airflow UI, puedes pasar JSON:
```json
{
  "environment": "production",
  "data_source": "api",
  "record_limit": 10000
}
```

O desde CLI:
```bash
airflow dags trigger context_challenge --conf '{"environment":"production","data_source":"database","record_limit":50000}'
```

**Context variables disponibles (principales):**
- `logical_date`: Fecha lógica del run
- `data_interval_start/end`: Intervalo de datos a procesar
- `run_id`: ID único del run
- `dag_run`: Objeto con info del run (incluye .conf para params)
- `task_instance`: Info de la tarea actual
- `dag`: Objeto DAG con metadata

**Configuración técnica:**
- DAG ID: `context_challenge`
- Schedule: @hourly
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'context', 'taskflow']`
- 6 funciones @task que usan **context y dag_run.conf
- Demuestra: runtime config, fechas, metadata, transformación adaptativa
"""

import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator

# Solución del challenge

@task(task_id='read_runtime_config')
def read_config(**context):
    """Lee parámetros pasados al triggear el DAG"""
    dag_run = context['dag_run']
    conf = dag_run.conf or {}  # dag_run.conf contiene params del trigger
    
    env = conf.get('environment', 'development')
    source = conf.get('data_source', 's3')
    limit = conf.get('record_limit', 100)
    
    print(f"Running in {env} mode, source: {source}, limit: {limit}")
    return {'env': env, 'source': source, 'limit': limit}

@task(task_id='process_date_range')
def process_dates(**context):
    """Accede a fechas del context de Airflow"""
    logical_date = context['logical_date']
    data_interval_start = context['data_interval_start']
    data_interval_end = context['data_interval_end']
    
    print(f"Processing data from {data_interval_start} to {data_interval_end}")
    print(f"Logical date: {logical_date}")
    
    start_str = data_interval_start.strftime('%Y-%m-%d %H:%M:%S')
    end_str = data_interval_end.strftime('%Y-%m-%d %H:%M:%S')
    
    return {'start': start_str, 'end': end_str}

@task(task_id='get_task_metadata')
def get_metadata(**context):
    """Accede a metadata de la tarea y DAG"""
    task_instance = context['task_instance']
    dag = context['dag']
    
    print(f"Task ID: {task_instance.task_id}")
    print(f"DAG ID: {dag.dag_id}")
    print(f"Try number: {task_instance.try_number}")
    print(f"Max tries: {task_instance.max_tries}")
    
    return {
        'task_id': task_instance.task_id,
        'dag_id': dag.dag_id,
        'try_number': task_instance.try_number
    }

@task(task_id='extract_data')
def extract_data(config, dates, **context):
    """Extrae datos usando configuración runtime y fechas"""
    env = config['env']
    source = config['source']
    limit = config['limit']
    start = dates['start']
    end = dates['end']
    
    print(f"Extracting from {source} in {env} mode")
    print(f"Date range: {start} to {end}")
    print(f"Record limit: {limit}")
    
    # Simulación: diferentes comportamientos según config
    if env == 'development':
        records = min(limit, 100)  # Sample pequeño en dev
    else:
        records = limit  # Full data en prod
    
    return {'records_extracted': records, 'source': source}

@task(task_id='transform_data')
def transform_data(extracted, config, **context):
    """Transforma datos adaptativamente según runtime config"""
    env = config['env']
    records = extracted['records_extracted']
    
    print(f"Transforming {records} records")
    
    # Validaciones exhaustivas solo en production
    if env == 'production':
        print("Running exhaustive validations (production mode)")
        validations = ['schema', 'nulls', 'duplicates', 'referential_integrity']
    else:
        print("Running basic validations (development mode)")
        validations = ['schema']
    
    return {
        'records_transformed': records,
        'validations_run': validations
    }

@task(task_id='load_data')
def load_data(transformed, config, metadata, **context):
    """Carga datos con metadata del contexto"""
    env = config['env']
    records = transformed['records_transformed']
    dag_id = metadata['dag_id']
    logical_date = context['logical_date']
    
    # Target diferente según environment
    if env == 'production':
        target = 'warehouse.production.fact_table'
    else:
        target = 'warehouse.dev.fact_table'
    
    print(f"Loading {records} records to {target}")
    print(f"Processed by DAG: {dag_id}")
    print(f"Logical date: {logical_date}")
    
    return {
        'records_loaded': records,
        'target': target,
        'status': 'success'
    }

with DAG(
    dag_id='context_challenge',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=['challenge', 'context', 'taskflow'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Leer configuración runtime
    config = read_config()
    
    # Procesar fechas del context
    dates = process_dates()
    
    # Obtener metadata
    metadata = get_metadata()
    
    # Pipeline con datos del context
    extracted = extract_data(config, dates)
    transformed = transform_data(extracted, config)
    loaded = load_data(transformed, config, metadata)
    
    end = EmptyOperator(task_id='end')
    
    start >> [config, dates, metadata] >> extracted >> transformed >> loaded >> end
