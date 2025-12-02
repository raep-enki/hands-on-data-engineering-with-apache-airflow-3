"""
XComs - Múltiples Keys por Tarea

Demuestra cómo una tarea puede pushear múltiples valores
usando diferentes keys, y otra tarea los pullea selectivamente.

Útil para separar datos principales de metadata.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def process_with_metadata(**context):
    """Procesa datos y guarda metadata separada"""
    ti = context['ti']
    logical_date = context['logical_date']
    
    print("⚙️ Procesando datos...")
    
    # Datos principales
    main_data = {
        'records': [
            {'id': 1, 'value': 100},
            {'id': 2, 'value': 200},
            {'id': 3, 'value': 150}
        ],
        'count': 3
    }
    
    # Metadata en keys separados
    ti.xcom_push(key='extraction_metadata', value={
        'source': 'api',
        'timestamp': str(datetime.datetime.now()),
        'date': str(logical_date.date())
    })
    
    ti.xcom_push(key='processing_stats', value={
        'duration_seconds': 12.5,
        'memory_mb': 45.3,
        'cpu_percent': 23.1
    })
    
    ti.xcom_push(key='validation_results', value={
        'passed': True,
        'errors': [],
        'warnings': ['Low memory']
    })
    
    print(f"✅ Procesados {main_data['count']} records")
    print("  - Metadata guardada en 3 keys separados")
    
    return main_data  # Key default: return_value


def use_selective_data(**context):
    """Usa solo los datos que necesita"""
    ti = context['ti']
    
    print("📥 Pulling datos selectivos...")
    
    # Pull solo lo necesario
    main_data = ti.xcom_pull(task_ids='process_task')
    extraction_meta = ti.xcom_pull(task_ids='process_task', key='extraction_metadata')
    
    # No pullamos processing_stats ni validation_results (no necesarios aquí)
    
    print(f"  - Main data: {main_data['count']} records")
    print(f"  - Source: {extraction_meta['source']}")
    print(f"  - Date: {extraction_meta['date']}")
    
    return {
        'processed': main_data['count'],
        'source': extraction_meta['source']
    }


def audit_complete_execution(**context):
    """Auditoria que necesita toda la metadata"""
    ti = context['ti']
    
    print("📊 Generando auditoria completa...")
    
    # Pull todo
    main_data = ti.xcom_pull(task_ids='process_task')
    extraction = ti.xcom_pull(task_ids='process_task', key='extraction_metadata')
    stats = ti.xcom_pull(task_ids='process_task', key='processing_stats')
    validation = ti.xcom_pull(task_ids='process_task', key='validation_results')
    
    audit = {
        'records_count': main_data['count'],
        'source': extraction['source'],
        'extraction_date': extraction['date'],
        'duration': stats['duration_seconds'],
        'validation_passed': validation['passed'],
        'warnings': validation['warnings']
    }
    
    print(f"✅ Audit: {audit}")
    return audit


with DAG(
    dag_id='xcoms_multiple_keys',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'xcoms']
) as dag:
    
    process = PythonOperator(
        task_id='process_task',
        python_callable=process_with_metadata
    )
    
    use_data = PythonOperator(
        task_id='use_data',
        python_callable=use_selective_data
    )
    
    audit = PythonOperator(
        task_id='audit',
        python_callable=audit_complete_execution
    )
    
    process >> [use_data, audit]

dag.doc_md = """
# Múltiples Keys por Tarea

## Push múltiples values:
```python
ti.xcom_push(key='data', value={...})
ti.xcom_push(key='metadata', value={...})
ti.xcom_push(key='stats', value={...})
return main_result  # key='return_value'
```

## Pull selectivo:
```python
# Solo lo necesario
data = ti.xcom_pull(task_ids='task', key='data')
meta = ti.xcom_pull(task_ids='task', key='metadata')
```

## Ventajas:
- Separación de concerns (data vs metadata)
- Pull selectivo (mejor performance)
- Keys semánticos (mejor legibilidad)
- Diferentes tareas usan diferentes keys
"""
