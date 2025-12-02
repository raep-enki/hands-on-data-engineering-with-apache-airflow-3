"""
XComs - TaskFlow API usa XCom automáticamente

Demuestra cómo TaskFlow API usa XCom internamente
para pasar datos entre tareas decoradas con @task.

Más limpio que PythonOperator con push/pull manual.
"""

import datetime

from airflow.sdk import DAG, task


@task
def extract():
    """TaskFlow automáticamente pushea return a XCom"""
    print("📥 Extrayendo datos...")
    
    data = {
        'users': [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ],
        'count': 2
    }
    
    print(f"✅ Extraídos {data['count']} users")
    # Return automáticamente → XCom push
    return data


@task
def transform(data: dict):
    """TaskFlow automáticamente pullea XCom como argumento"""
    print(f"⚙️ Transformando {data['count']} users...")
    
    # data viene automáticamente del XCom de extract()
    for user in data['users']:
        user['name_upper'] = user['name'].upper()
    
    print("✅ Transformación completa")
    # Return automáticamente → XCom push
    return data


@task
def load(data: dict):
    """TaskFlow pullea XCom automáticamente"""
    print(f"💾 Cargando {data['count']} users...")
    
    for user in data['users']:
        print(f"  - {user['name_upper']}")
    
    print("✅ Carga completa")
    return {'status': 'success', 'loaded': data['count']}


@task
def manual_xcom_with_taskflow(ti):
    """Incluso con TaskFlow, puedes usar XCom manual"""
    print("🔧 Acceso manual a XComs desde TaskFlow...")
    
    # Pull manual de tasks anteriores
    extract_data = ti.xcom_pull(task_ids='extract')
    load_result = ti.xcom_pull(task_ids='load')
    
    print(f"  - Extract retornó: {extract_data}")
    print(f"  - Load retornó: {load_result}")
    
    # Push manual adicional
    ti.xcom_push(key='audit_metadata', value={
        'extracted_count': extract_data['count'],
        'load_status': load_result['status']
    })
    
    return {'audit': 'complete'}


with DAG(
    dag_id='xcoms_taskflow_automatic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'xcoms']
) as dag:
    
    # TaskFlow: XCom automático
    data = extract()
    transformed = transform(data)
    result = load(transformed)
    audit = manual_xcom_with_taskflow()
    
    result >> audit

dag.doc_md = """
# TaskFlow API + XCom

## TaskFlow maneja XCom automáticamente:

```python
@task
def task_a():
    return {"value": 100}  # ✅ Auto push a XCom

@task
def task_b(data: dict):  # ✅ Auto pull de XCom
    print(data)  # {"value": 100}

data = task_a()
task_b(data)  # Dependencia + XCom automático
```

## Equivalente sin TaskFlow:

```python
def task_a_func(**context):
    ti = context['ti']
    data = {"value": 100}
    return data  # Push manual

def task_b_func(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='task_a')  # Pull manual
    print(data)

task_a = PythonOperator(task_id='task_a', python_callable=task_a_func)
task_b = PythonOperator(task_id='task_b', python_callable=task_b_func)
task_a >> task_b
```

## XCom manual en TaskFlow:
Puedes mezclar: TaskFlow para simplicidad + XCom manual cuando necesario.

```python
@task
def my_task(data: dict, ti):  # data auto, ti manual
    # Auto pull
    print(data)
    
    # Manual push
    ti.xcom_push(key='metadata', value={...})
```

## Ventaja de TaskFlow:
- Menos código boilerplate
- Más pythónico
- Dependencies implícitas
- Type hints claros
"""
