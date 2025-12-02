"""
Params - Params con dag_run.conf Override

Params pueden ser sobrescritos al triggear DAG usando conf.
dag_run.conf tiene prioridad sobre params defaults.
"""

import datetime

from airflow.sdk import DAG, task


@task
def check_params_vs_conf(**context):
    """Compara params defaults vs conf runtime"""
    params = context['params']
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    
    print("📋 Params (defaults):")
    print(f"  - region: {params['region']}")
    print(f"  - batch_size: {params['batch_size']}")
    
    print("\n⚙️ Runtime conf:")
    print(f"  - conf: {conf}")
    
    # Params merge con conf
    # Si conf tiene valores, override params
    final_region = conf.get('region', params['region'])
    final_batch = conf.get('batch_size', params['batch_size'])
    
    print("\n✅ Final values:")
    print(f"  - region: {final_region}")
    print(f"  - batch_size: {final_batch}")
    
    return {
        'region': final_region,
        'batch_size': final_batch
    }


with DAG(
    dag_id='params_with_conf_override',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    params={
        'region': 'us-east-1',
        'batch_size': 1000,
        'enable_cache': True
    },
    tags=['example', 'core_concepts', 'params']
) as dag:
    
    config = check_params_vs_conf()

dag.doc_md = """
# Params + dag_run.conf

## Defaults en DAG:
```python
params={'region': 'us-east-1', 'batch_size': 1000}
```

## Override al trigger:
```bash
airflow dags trigger my_dag --conf '{
  "region": "eu-west-1",
  "batch_size": 5000
}'
```

## En tarea:
```python
params = context['params']  # Defaults
conf = context['dag_run'].conf  # Runtime override
final = conf.get('key', params['key'])  # Merged
```

Params proveen defaults, conf permite customización runtime.
"""
