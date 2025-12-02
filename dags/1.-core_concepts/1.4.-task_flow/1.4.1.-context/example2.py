"""
TaskFlow - Context en @task

Demuestra cómo acceder al contexto de Airflow dentro de @task.
El contexto contiene metadata sobre el DAG run, task instance, etc.

Usa **context o parámetros individuales.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator


@task
def task_with_full_context(**context):
    """Accede al contexto completo usando **context"""
    print("📋 Contexto completo:")
    
    # Metadata del DAG run
    logical_date = context['logical_date']
    print(f"  - logical_date: {logical_date}")
    
    # Metadata de la tarea
    task_instance = context['ti']
    print(f"  - task_id: {task_instance.task_id}")
    print(f"  - dag_id: {task_instance.dag_id}")
    print(f"  - run_id: {context['run_id']}")
    
    # DAG object
    dag = context['dag']
    print(f"  - dag.schedule: {dag.schedule}")
    
    return f"Processed at {logical_date}"


@task
def task_with_specific_params(logical_date, ti, dag_run):
    """Accede a parámetros específicos del contexto"""
    print("🎯 Parámetros específicos:")
    print(f"  - logical_date: {logical_date}")
    print(f"  - task_id: {ti.task_id}")
    print(f"  - dag_run.run_id: {dag_run.run_id}")
    
    return {
        "date": str(logical_date),
        "task": ti.task_id
    }


@task
def task_mixing_data_and_context(data: dict, **context):
    """Combina datos de tarea anterior con contexto"""
    print("🔀 Datos + Contexto:")
    print(f"  - Datos recibidos: {data}")
    print(f"  - Ejecutado en: {context['logical_date']}")
    
    # Usar ambos
    result = {
        **data,
        "processed_at": str(context['logical_date']),
        "task_id": context['ti'].task_id
    }
    
    return result


with DAG(
    dag_id='taskflow_context_access',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'task_flow', 'context']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Tareas con contexto
    result1 = task_with_full_context()
    result2 = task_with_specific_params()
    result3 = task_mixing_data_and_context(result2)
    
    end = EmptyOperator(task_id='end')
    
    start >> result1 >> result2 >> result3 >> end

dag.doc_md = """
# Contexto en TaskFlow API

**Context**: Diccionario con metadata sobre ejecución de DAG/Task.

## Métodos de acceso:

### 1. Full context con **context

```python
@task
def my_task(**context):
    # Acceso a todo el contexto
    logical_date = context['logical_date']
    ti = context['ti']
    dag = context['dag']
```

### 2. Parámetros específicos

```python
@task
def my_task(logical_date, ti, dag_run):
    # Solo los que necesitas
    print(logical_date)
    print(ti.task_id)
    print(dag_run.run_id)
```

### 3. Combinado con datos

```python
@task
def my_task(data: dict, **context):
    # Primer arg: datos de tarea anterior
    # **context: metadata de Airflow
    logical_date = context['logical_date']
    return {"data": data, "date": logical_date}
```

## Context keys disponibles:

### Temporal
- **logical_date**: Fecha lógica del DAG run (antes execution_date)
- **data_interval_start**: Inicio del intervalo de datos
- **data_interval_end**: Fin del intervalo de datos
- **next_execution_date**: Próxima ejecución programada
- **prev_execution_date**: Ejecución anterior

### Task Instance
- **ti**: Objeto TaskInstance actual
- **task_instance**: Alias de ti
- **task**: Objeto Task
- **run_id**: ID único del DAG run

### DAG
- **dag**: Objeto DAG
- **dag_run**: Objeto DagRun
- **conf**: Configuración pasada al DAG run

### Airflow internals
- **params**: Parámetros del DAG
- **var**: Variables de Airflow
- **conn**: Conexiones
- **test_mode**: Si está en modo test

## Uso de logical_date:

```python
@task
def process_data(logical_date):
    # Procesar datos para esta fecha
    print(f"Processing data for {logical_date.date()}")
    
    # Formato para queries
    date_str = logical_date.strftime('%Y-%m-%d')
    query = f"SELECT * FROM table WHERE date = '{date_str}'"
    
    return query
```

## Uso de TaskInstance (ti):

```python
@task
def use_ti(ti):
    # Metadata de la tarea
    print(f"Task ID: {ti.task_id}")
    print(f"DAG ID: {ti.dag_id}")
    print(f"Try number: {ti.try_number}")
    print(f"Max tries: {ti.max_tries}")
    
    # XCom manual (TaskFlow hace esto automáticamente)
    ti.xcom_push(key='custom_key', value='custom_value')
    value = ti.xcom_pull(task_ids='other_task', key='custom_key')
```

## Uso de dag_run:

```python
@task
def use_dag_run(dag_run):
    # Metadata del DAG run
    print(f"Run ID: {dag_run.run_id}")
    print(f"Run type: {dag_run.run_type}")  # manual, scheduled, backfill
    print(f"External trigger: {dag_run.external_trigger}")
    
    # Configuración pasada al run
    conf = dag_run.conf or {}
    environment = conf.get('environment', 'prod')
    print(f"Environment: {environment}")
```

## Uso de conf (runtime config):

```python
@task
def use_conf(**context):
    # Config pasado al triggear DAG manualmente
    conf = context.get('dag_run').conf or {}
    
    # Valores con defaults
    region = conf.get('region', 'us-east-1')
    batch_size = conf.get('batch_size', 1000)
    
    print(f"Processing region: {region}")
    print(f"Batch size: {batch_size}")
    
    return {"region": region, "batch_size": batch_size}

# Trigger con config:
# airflow dags trigger my_dag --conf '{"region": "eu-west-1", "batch_size": 5000}'
```

## Data intervals:

```python
@task
def process_interval(data_interval_start, data_interval_end):
    print(f"Processing data from {data_interval_start} to {data_interval_end}")
    
    # Para DAG @daily:
    # logical_date: 2024-01-15 00:00
    # data_interval_start: 2024-01-15 00:00
    # data_interval_end: 2024-01-16 00:00
    
    # Útil para queries:
    query = f\"\"\"
    SELECT * FROM events
    WHERE timestamp >= '{data_interval_start}'
      AND timestamp < '{data_interval_end}'
    \"\"\"
    return query
```

## Orden de parámetros:

```python
@task
def task_with_multiple_params(
    data: dict,           # Primer arg: datos de tarea previa
    logical_date,         # Context params
    ti,
    **context            # Resto del context
):
    # data viene de otra tarea
    # logical_date, ti son context params
    # context tiene el resto
    pass

prev_data = previous_task()
task_with_multiple_params(prev_data)
```

## Best practices:

1. **Solo pide lo que necesitas**: No uses **context si solo necesitas logical_date
2. **Type hints**: Ayudan a IDEs y documentación
3. **Defaults**: Usa .get() para keys opcionales
4. **logical_date vs now()**: logical_date es para datos, now() para timestamps
5. **Naming**: logical_date en Airflow 3.x (antes execution_date)

## Ejemplo completo:

```python
@task
def etl_task(
    input_data: dict,      # De tarea anterior
    logical_date,          # Context param
    ti,                    # Context param
    dag_run,               # Context param
    **context             # Resto del context
):
    # Input data
    print(f"Input: {input_data}")
    
    # Temporal context
    date_str = logical_date.strftime('%Y-%m-%d')
    print(f"Processing date: {date_str}")
    
    # Task metadata
    print(f"Task: {ti.task_id}")
    print(f"Attempt: {ti.try_number}")
    
    # DAG run metadata
    run_type = dag_run.run_type
    print(f"Run type: {run_type}")
    
    # Runtime config
    conf = dag_run.conf or {}
    env = conf.get('env', 'prod')
    
    result = {
        "data": input_data,
        "date": date_str,
        "task": ti.task_id,
        "env": env
    }
    
    return result
```
"""
