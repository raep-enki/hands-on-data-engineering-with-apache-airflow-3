"""
XComs - Introducción a XCom Push/Pull

XCom (Cross-Communication) permite compartir datos entre tareas.
Es el mecanismo subyacente usado por TaskFlow API automáticamente.

Este ejemplo muestra el uso explícito de XCom.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def push_data(**context):
    """Pushea datos a XCom"""
    ti = context['ti']
    
    print("📤 Pushing datos a XCom...")
    
    # Push con key default (return_value)
    data = {"users": 100, "orders": 500}
    
    # Push manual con key custom
    ti.xcom_push(key='metadata', value={'version': '1.0', 'source': 'api'})
    ti.xcom_push(key='stats', value={'success': True, 'duration': 12.5})
    
    print(f"✅ Data pushed: {data}")
    return data  # También se guarda en XCom automáticamente


def pull_data(**context):
    """Pullea datos desde XCom"""
    ti = context['ti']
    
    print("📥 Pulling datos desde XCom...")
    
    # Pull del return (key default)
    data = ti.xcom_pull(task_ids='push_task')
    print(f"  - Data (return): {data}")
    
    # Pull con keys custom
    metadata = ti.xcom_pull(task_ids='push_task', key='metadata')
    stats = ti.xcom_pull(task_ids='push_task', key='stats')
    
    print(f"  - Metadata: {metadata}")
    print(f"  - Stats: {stats}")
    
    return {"pulled_count": len(data)}


with DAG(
    dag_id='xcoms_basic_push_pull',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'xcoms']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_data
    )
    
    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_data
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> push_task >> pull_task >> end

dag.doc_md = """
# XCom Basics

**XCom**: Mecanismo para compartir pequeños datos entre tareas.

## Push:
```python
def my_task(**context):
    ti = context['ti']
    ti.xcom_push(key='my_key', value={'data': 123})
    return "value"  # También push automático
```

## Pull:
```python
def another_task(**context):
    ti = context['ti']
    value = ti.xcom_pull(task_ids='my_task', key='my_key')
```

## Key default:
El `return` usa key='return_value' automáticamente.

## Limitaciones:
- Tamaño pequeño (< 1 MB típicamente)
- Solo datos serializables (JSON)
- Almacenado en base de datos metadata
"""
