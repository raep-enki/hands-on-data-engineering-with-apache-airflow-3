"""
Params - Params Complejos (Listas y Objetos)

Params pueden ser tipos complejos: listas, dicts nested.
Útil para configuración estructurada.
"""

import datetime

from airflow.sdk import DAG, task


@task
def process_list_param(**context):
    """Procesa param tipo lista"""
    params = context['params']
    sources = params['data_sources']
    
    print(f"📊 Procesando {len(sources)} sources:")
    
    results = []
    for source in sources:
        print(f"  - Extrayendo de: {source}")
        # Simulate extraction
        results.append({'source': source, 'records': 100})
    
    return {'sources_processed': len(sources), 'results': results}


@task
def process_object_param(**context):
    """Procesa param tipo objeto nested"""
    params = context['params']
    db_config = params['database_config']
    
    print(f"🗄️ Usando config de DB:")
    print(f"  - Host: {db_config['host']}")
    print(f"  - Port: {db_config['port']}")
    print(f"  - Database: {db_config['database']}")
    print(f"  - Pool size: {db_config['pool_size']}")
    
    connection_string = f"postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
    
    return {'connection': connection_string}


with DAG(
    dag_id='params_complex_types',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    params={
        'data_sources': ['api', 'database', 's3'],  # Lista
        'database_config': {  # Objeto nested
            'host': 'localhost',
            'port': 5432,
            'database': 'analytics',
            'pool_size': 10
        },
        'processing_options': {
            'parallel': True,
            'retry_count': 3,
            'timeout': 300
        }
    },
    tags=['example', 'params']
) as dag:
    
    list_result = process_list_param()
    object_result = process_object_param()

dag.doc_md = """
# Params Complejos

## Listas:
```python
params={'sources': ['api', 'db', 's3']}

@task
def process(**context):
    sources = context['params']['sources']
    for source in sources:
        extract(source)
```

## Objetos nested:
```python
params={
    'config': {
        'db': {'host': 'localhost', 'port': 5432},
        'api': {'url': 'https://api.com', 'timeout': 30}
    }
}

@task
def process(**context):
    config = context['params']['config']
    db_host = config['db']['host']
```

## Override al trigger:
```bash
airflow dags trigger my_dag --conf '{
  "sources": ["api", "warehouse"],
  "config": {"db": {"host": "prod-db.com"}}
}'
```
"""
