"""
TaskFlow - Fan-Out/Fan-In con Objetos

Demuestra patrón fan-out (una tarea genera múltiples outputs)
y fan-in (múltiples tareas convergen en una).

Útil para procesamiento paralelo de datos.
"""

import datetime

from airflow.sdk import DAG, task


@task
def split_data() -> dict:
    """Divide datos en batches"""
    print("🔀 Dividiendo datos en batches...")
    
    all_data = list(range(1, 101))  # 100 items
    
    batches = {
        'batch_1': all_data[0:33],
        'batch_2': all_data[33:66],
        'batch_3': all_data[66:100]
    }
    
    print(f"✅ Dividido en {len(batches)} batches")
    return batches


@task
def process_batch_1(batches: dict) -> dict:
    """Procesa batch 1"""
    batch = batches['batch_1']
    print(f"⚙️ Procesando batch 1: {len(batch)} items")
    
    result = sum(batch)
    return {'batch': 1, 'sum': result, 'count': len(batch)}


@task
def process_batch_2(batches: dict) -> dict:
    """Procesa batch 2"""
    batch = batches['batch_2']
    print(f"⚙️ Procesando batch 2: {len(batch)} items")
    
    result = sum(batch)
    return {'batch': 2, 'sum': result, 'count': len(batch)}


@task
def process_batch_3(batches: dict) -> dict:
    """Procesa batch 3"""
    batch = batches['batch_3']
    print(f"⚙️ Procesando batch 3: {len(batch)} items")
    
    result = sum(batch)
    return {'batch': 3, 'sum': result, 'count': len(batch)}


@task
def aggregate_results(result1: dict, result2: dict, result3: dict) -> dict:
    """Agrega resultados de todos los batches"""
    print("📊 Agregando resultados...")
    
    total_sum = result1['sum'] + result2['sum'] + result3['sum']
    total_count = result1['count'] + result2['count'] + result3['count']
    
    results = {
        'batches': [result1, result2, result3],
        'total_sum': total_sum,
        'total_count': total_count,
        'average': total_sum / total_count
    }
    
    print(f"✅ Total sum: {total_sum}, Average: {results['average']:.2f}")
    return results


with DAG(
    dag_id='taskflow_fan_out_fan_in',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'task_flow', 'passing_arbitrary_objects_as_arguments']
) as dag:
    
    # Fan-out: una tarea → múltiples tareas
    batches = split_data()
    r1 = process_batch_1(batches)
    r2 = process_batch_2(batches)
    r3 = process_batch_3(batches)
    
    # Fan-in: múltiples tareas → una tarea
    final = aggregate_results(r1, r2, r3)

dag.doc_md = """
# Fan-Out/Fan-In Pattern

## Fan-Out (1 → N):
```python
@task
def split():
    return {'batch1': [...], 'batch2': [...]}

data = split()
r1 = process_batch_1(data)
r2 = process_batch_2(data)
r3 = process_batch_3(data)
```

## Fan-In (N → 1):
```python
@task
def aggregate(r1, r2, r3):
    return combine(r1, r2, r3)

result = aggregate(r1, r2, r3)
```

Útil para procesamiento paralelo.
"""
