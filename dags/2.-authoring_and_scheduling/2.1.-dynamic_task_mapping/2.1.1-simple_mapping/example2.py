"""
Dynamic Task Mapping - expand() con Múltiples Argumentos

expand() puede mapear múltiples argumentos simultáneamente.
Útil cuando cada task instance necesita múltiples inputs.
"""

import datetime

from airflow.sdk import DAG, task


@task
def get_config():
    """Retorna configuración para cada proceso"""
    print("⚙️ Generando configuración...")
    
    configs = [
        {'region': 'us-east-1', 'batch_size': 1000, 'timeout': 300},
        {'region': 'us-west-2', 'batch_size': 1500, 'timeout': 400},
        {'region': 'eu-west-1', 'batch_size': 1200, 'timeout': 350}
    ]
    
    print(f"✅ Configuración para {len(configs)} regiones")
    return configs


@task
def process_region(config: dict):
    """Procesa una región con su configuración"""
    region = config['region']
    batch_size = config['batch_size']
    timeout = config['timeout']
    
    print(f"🌍 Procesando región: {region}")
    print(f"  - Batch size: {batch_size}")
    print(f"  - Timeout: {timeout}s")
    
    # Simular procesamiento
    records_processed = batch_size * 5
    
    return {
        'region': region,
        'records': records_processed,
        'status': 'success'
    }


@task
def summarize_regions(results: list):
    """Resume resultados de todas las regiones"""
    print(f"📊 Resumiendo {len(results)} regiones...")
    
    total_records = sum(r['records'] for r in results)
    regions = [r['region'] for r in results]
    
    print(f"✅ Regiones procesadas: {', '.join(regions)}")
    print(f"✅ Total records: {total_records}")
    
    return {
        'regions_count': len(regions),
        'total_records': total_records,
        'details': results
    }


with DAG(
    dag_id='dynamic_mapping_multiple_args',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'dynamic_task_mapping', 'simple_mapping']
) as dag:
    
    configs = get_config()
    
    # Dynamic mapping con dict completo
    processed = process_region.expand(config=configs)
    
    summary = summarize_regions(processed)

dag.doc_md = """
# expand() con Múltiples Argumentos

## Pasar dict completo:
```python
@task
def get_configs():
    return [
        {'region': 'us-east-1', 'size': 1000},
        {'region': 'us-west-2', 'size': 1500}
    ]

@task
def process(config: dict):
    region = config['region']
    size = config['size']
    # Process...

configs = get_configs()
process.expand(config=configs)
```

## Múltiples expand():
```python
@task
def process(region: str, size: int):
    # Process...

regions = get_regions()  # ['us-east-1', 'us-west-2']
sizes = get_sizes()      # [1000, 1500]

# Crea cartesian product
process.expand(region=regions, size=sizes)
# Result: 2 x 2 = 4 task instances
```

## Partial (valores fijos):
```python
@task
def process(item: str, timeout: int):
    # Process...

items = get_items()  # ['a', 'b', 'c']

# timeout fijo, item varía
process.partial(timeout=300).expand(item=items)
```
"""
