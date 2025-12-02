"""
TaskFlow - Pasar Listas y Estructuras Nested

Demuestra cómo pasar listas y estructuras anidadas
entre tareas usando TaskFlow API.

Las estructuras se serializan automáticamente.
"""

import datetime

from airflow.sdk import DAG, task


@task
def extract_orders() -> list:
    """Extrae lista de órdenes"""
    print("📦 Extrayendo órdenes...")
    
    orders = [
        {'order_id': 101, 'user_id': 1, 'amount': 250.50, 'items': ['laptop', 'mouse']},
        {'order_id': 102, 'user_id': 1, 'amount': 15.99, 'items': ['cable']},
        {'order_id': 103, 'user_id': 2, 'amount': 999.00, 'items': ['phone', 'case', 'charger']}
    ]
    
    print(f"✅ Extraídas {len(orders)} órdenes")
    return orders


@task
def calculate_stats(orders: list) -> dict:
    """Calcula estadísticas de lista de órdenes"""
    print(f"📊 Calculando estadísticas de {len(orders)} órdenes...")
    
    total_amount = sum(order['amount'] for order in orders)
    total_items = sum(len(order['items']) for order in orders)
    avg_amount = total_amount / len(orders)
    
    stats = {
        'order_count': len(orders),
        'total_amount': total_amount,
        'avg_amount': round(avg_amount, 2),
        'total_items': total_items
    }
    
    print(f"✅ Stats: {stats}")
    return stats


@task
def enrich_orders(orders: list, stats: dict) -> dict:
    """Enriquece órdenes con estadísticas"""
    print("⚙️ Enriqueciendo órdenes con stats...")
    
    enriched = []
    for order in orders:
        enriched.append({
            **order,
            'percentage_of_total': round((order['amount'] / stats['total_amount']) * 100, 2),
            'item_count': len(order['items'])
        })
    
    return {
        'orders': enriched,
        'stats': stats
    }


@task
def generate_report(data: dict) -> dict:
    """Genera reporte final"""
    print("📄 Generando reporte...")
    
    orders = data['orders']
    stats = data['stats']
    
    print("\n=== REPORTE DE ÓRDENES ===")
    print(f"Total órdenes: {stats['order_count']}")
    print(f"Monto total: ${stats['total_amount']}")
    print(f"Promedio: ${stats['avg_amount']}")
    print("\nDetalle por orden:")
    
    for order in orders:
        print(f"  Order {order['order_id']}: ${order['amount']} ({order['percentage_of_total']}%) - {order['item_count']} items")
    
    return {'status': 'report_generated', 'order_count': len(orders)}


with DAG(
    dag_id='taskflow_pass_lists',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'task_flow', 'passing_arbitrary_objects_as_arguments']
) as dag:
    
    # Pipeline con listas y nested dicts
    orders = extract_orders()
    stats = calculate_stats(orders)
    enriched = enrich_orders(orders, stats)
    report = generate_report(enriched)

dag.doc_md = """
# Pasar Listas y Estructuras Nested

## Listas:
```python
@task
def task_a() -> list:
    return [1, 2, 3]

@task
def task_b(numbers: list):
    return sum(numbers)
```

## Múltiples argumentos:
```python
@task
def task_a() -> list:
    return [1, 2, 3]

@task
def task_b() -> dict:
    return {'key': 'value'}

@task
def task_c(numbers: list, data: dict):
    # Recibe de ambas tareas
    pass

nums = task_a()
data = task_b()
task_c(nums, data)  # Múltiples inputs
```

## Nested structures:
```python
@task
def complex():
    return {
        'users': [
            {'id': 1, 'orders': [101, 102]},
            {'id': 2, 'orders': [103]}
        ],
        'metadata': {'count': 2}
    }
```
"""
