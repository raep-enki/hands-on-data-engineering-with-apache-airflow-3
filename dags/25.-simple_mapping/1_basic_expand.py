"""
Dynamic Task Mapping - Introducción a expand()

Dynamic Task Mapping permite crear tareas dinámicamente
en runtime basándose en datos. Usa el método expand().

Introducido en Airflow 2.3+, reemplaza loops manuales.
"""

import datetime

from airflow.sdk import DAG, task


@task
def get_items():
    """Retorna lista de items a procesar"""
    print("📋 Generando lista de items...")
    
    items = [
        {'id': 1, 'name': 'item_a', 'value': 100},
        {'id': 2, 'name': 'item_b', 'value': 200},
        {'id': 3, 'name': 'item_c', 'value': 150}
    ]
    
    print(f"✅ Generados {len(items)} items")
    return items


@task
def process_item(item: dict):
    """Procesa un item individual"""
    print(f"⚙️ Procesando item {item['id']}: {item['name']}")
    print(f"  - Value: {item['value']}")
    
    result = {
        'id': item['id'],
        'name': item['name'],
        'processed_value': item['value'] * 2
    }
    
    print(f"✅ Item procesado: {result}")
    return result


@task
def aggregate_results(results: list):
    """Agrega resultados de todos los items procesados"""
    print(f"📊 Agregando {len(results)} resultados...")
    
    total_value = sum(r['processed_value'] for r in results)
    
    summary = {
        'total_items': len(results),
        'total_value': total_value,
        'items': results
    }
    
    print(f"✅ Total value: {total_value}")
    return summary


with DAG(
    dag_id='dynamic_mapping_basic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'simple_mapping']
) as dag:
    
    # Get items to process
    items = get_items()
    
    # Dynamic mapping: crea una task instance por cada item
    processed = process_item.expand(item=items)
    
    # Aggregate all results
    summary = aggregate_results(processed)

dag.doc_md = """
# Dynamic Task Mapping

Crea tareas dinámicamente en runtime según datos.

## Sin dynamic mapping (antiguo):
```python
# ❌ Loop manual (no dinámico)
for i in range(3):
    task = PythonOperator(
        task_id=f'process_{i}',
        python_callable=process
    )
```

## Con dynamic mapping (nuevo):
```python
# ✅ Dynamic mapping
@task
def get_items():
    return [1, 2, 3]

@task
def process(item):
    return item * 2

items = get_items()
processed = process.expand(item=items)  # 3 task instances
```

## Ventajas:
- Número de tasks determinado en runtime
- Paralelización automática
- Mejor UI (grouped tasks)
- Retry individual por item

## expand():
- Crea una task instance por elemento
- Paraleliza según worker capacity
- Results son lista ordenada

## Casos de uso:
- Procesar lista de archivos
- Batch processing
- Fan-out patterns
- Data partitioning
"""
