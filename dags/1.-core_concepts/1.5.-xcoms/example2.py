"""
XComs - Pull de Múltiples Tareas

Demuestra cómo pullear XComs de múltiples tareas upstream.
Útil para agregaciones y fan-in patterns.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def extract_source_a(**context):
    """Extrae de fuente A"""
    print("📥 Extrayendo de fuente A...")
    data = {"source": "A", "records": 150, "status": "success"}
    print(f"✅ Fuente A: {data['records']} records")
    return data


def extract_source_b(**context):
    """Extrae de fuente B"""
    print("📥 Extrayendo de fuente B...")
    data = {"source": "B", "records": 200, "status": "success"}
    print(f"✅ Fuente B: {data['records']} records")
    return data


def extract_source_c(**context):
    """Extrae de fuente C"""
    print("📥 Extrayendo de fuente C...")
    data = {"source": "C", "records": 180, "status": "success"}
    print(f"✅ Fuente C: {data['records']} records")
    return data


def aggregate_all_sources(**context):
    """Agrega datos de todas las fuentes"""
    ti = context['ti']
    
    print("📊 Agregando datos de todas las fuentes...")
    
    # Pull de múltiples tareas - retorna lista
    all_data = ti.xcom_pull(task_ids=['extract_a', 'extract_b', 'extract_c'])
    
    print(f"  - Datos recibidos de {len(all_data)} fuentes")
    
    # Calcular totales
    total_records = sum(d['records'] for d in all_data if d)
    sources = [d['source'] for d in all_data if d]
    all_success = all(d['status'] == 'success' for d in all_data if d)
    
    result = {
        'sources': sources,
        'total_records': total_records,
        'source_count': len(sources),
        'all_success': all_success
    }
    
    print(f"✅ Total: {total_records} records from {len(sources)} sources")
    return result


with DAG(
    dag_id='xcoms_multiple_pull',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'xcoms']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Extracciones paralelas
    extract_a = PythonOperator(
        task_id='extract_a',
        python_callable=extract_source_a
    )
    
    extract_b = PythonOperator(
        task_id='extract_b',
        python_callable=extract_source_b
    )
    
    extract_c = PythonOperator(
        task_id='extract_c',
        python_callable=extract_source_c
    )
    
    # Agregación
    aggregate = PythonOperator(
        task_id='aggregate',
        python_callable=aggregate_all_sources
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> [extract_a, extract_b, extract_c] >> aggregate >> end

dag.doc_md = """
# XCom Pull de Múltiples Tareas

## Pull de lista de tasks:
```python
ti.xcom_pull(task_ids=['task1', 'task2', 'task3'])
# Retorna: [value1, value2, value3]
```

## Usar en agregaciones:
```python
all_data = ti.xcom_pull(task_ids=['task1', 'task2'])
total = sum(d['value'] for d in all_data)
```

## Orden:
Los valores retornan en el mismo orden que task_ids list.

## None handling:
Si tarea no tiene XCom, retorna None en esa posición.
"""
