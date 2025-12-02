"""
Dynamic Task Mapping - map() en Task Groups

Dynamic mapping puede usarse con task groups
para crear grupos de tareas dinámicamente.
"""

import datetime

from airflow.sdk import DAG, task, task_group


@task
def get_databases():
    """Lista de bases de datos a procesar"""
    print("🗄️ Generando lista de databases...")
    
    databases = ['db_users', 'db_orders', 'db_products']
    
    print(f"✅ {len(databases)} databases")
    return databases


@task_group
def process_database(database: str):
    """Grupo de tareas para procesar una database"""
    
    @task
    def extract(db: str):
        print(f"📥 Extracting from {db}...")
        return {'db': db, 'records': 1000}
    
    @task
    def transform(data: dict):
        print(f"⚙️ Transforming data from {data['db']}...")
        data['records'] = data['records'] * 2
        return data
    
    @task
    def load(data: dict):
        print(f"💾 Loading data from {data['db']}...")
        return {'db': data['db'], 'status': 'loaded'}
    
    # Pipeline interno del grupo
    data = extract(database)
    transformed = transform(data)
    result = load(transformed)
    
    return result


@task
def summarize_all(results: list):
    """Resume todos los databases procesados"""
    print(f"📊 Resumiendo {len(results)} databases...")
    
    dbs = [r['db'] for r in results]
    
    print(f"✅ Databases procesados: {', '.join(dbs)}")
    return {'databases_count': len(dbs), 'all_success': True}


with DAG(
    dag_id='dynamic_mapping_task_groups',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'authoring_and_scheduling', 'dynamic_task_mapping', 'simple_mapping']
) as dag:
    
    databases = get_databases()
    
    # Dynamic mapping de task group
    processed = process_database.expand(database=databases)
    
    summary = summarize_all(processed)

dag.doc_md = """
# Dynamic Mapping con Task Groups

## Task Group dinámico:
```python
@task_group
def process_item(item: str):
    @task
    def extract(x: str):
        return extract_data(x)
    
    @task
    def load(data):
        return load_data(data)
    
    data = extract(item)
    load(data)

items = get_items()

# Crea un task group por cada item
process_item.expand(item=items)
```

## Resultado en UI:
```
process_item.extract[0]
process_item.extract[1]
process_item.extract[2]

process_item.load[0]
process_item.load[1]
process_item.load[2]
```

## Ventajas:
- Agrupación lógica visual
- Pipeline completo por item
- Retry de grupo completo o task individual
- Mejor organización en UI

## Caso de uso:
Cuando cada item necesita múltiples steps.
"""
