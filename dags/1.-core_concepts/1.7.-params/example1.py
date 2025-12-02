"""
Params - Introducción a DAG Params

Params permiten definir parámetros configurables por DAG
que pueden ser sobrescritos al triggear el DAG manualmente.

Diferencia con Variables: Params son por DAG, Variables globales.
"""

import datetime

from airflow.sdk import DAG, task


@task
def read_params(**context):
    """Lee params del DAG"""
    params = context['params']
    
    print("📋 Leyendo params...")
    print(f"  - Environment: {params['environment']}")
    print(f"  - Batch size: {params['batch_size']}")
    print(f"  - Debug mode: {params['debug']}")
    
    return {
        'environment': params['environment'],
        'batch_size': params['batch_size'],
        'debug': params['debug']
    }


@task
def process_with_params(config: dict):
    """Procesa usando params"""
    print(f"⚙️ Procesando con config: {config}")
    
    if config['debug']:
        print("  🐛 Debug mode: verbose logging")
    
    batch_size = config['batch_size']
    print(f"  📦 Batch size: {batch_size}")
    
    # Simular procesamiento
    total_records = batch_size * 10
    
    return {
        'records_processed': total_records,
        'batch_size': batch_size
    }


with DAG(
    dag_id='params_basic_usage',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    # Definir params con defaults (dict simple compatible con Airflow 3.x)
    params={
        'environment': 'production',
        'batch_size': 1000,
        'debug': False
    },
    tags=['example', 'core_concepts', 'params']
) as dag:
    
    config = read_params()
    result = process_with_params(config)

dag.doc_md = """
# DAG Params

Params definen parámetros configurables por DAG con defaults.

## Definir params:
```python
with DAG(
    dag_id='my_dag',
    params={
        'environment': 'production',
        'batch_size': 1000,
        'debug': False
    }
):
    pass
```

## Leer params:
```python
@task
def my_task(**context):
    params = context['params']
    env = params['environment']
    size = params['batch_size']
```

## Trigger con params custom:

### UI:
1. DAGs → my_dag → Trigger DAG
2. Modificar params en el form
3. Trigger

### CLI:
```bash
airflow dags trigger my_dag \\
  --conf '{"environment": "staging", "batch_size": 500}'
```

## Params vs Variables:
- **Params**: Por DAG, defaults en código, runtime override
- **Variables**: Global, persiste en DB, compartido entre DAGs

## Tipos soportados:
- string
- integer
- boolean
- number (float)
- array
- object
"""
