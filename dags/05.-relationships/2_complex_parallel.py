"""
Task Relationships - Dependencias Complejas

Demuestra patrones avanzados de dependencias:
múltiples fan-out/fan-in, cross-dependencies.

Pattern común en ETL enterprise.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_relationships_complex',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'relationships']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Layer 1: Extracciones en paralelo
    extract_orders = BashOperator(
        task_id='extract_orders',
        bash_command='echo "📥 Orders"'
    )
    
    extract_customers = BashOperator(
        task_id='extract_customers',
        bash_command='echo "📥 Customers"'
    )
    
    extract_products = BashOperator(
        task_id='extract_products',
        bash_command='echo "📥 Products"'
    )
    
    # Layer 2: Transformaciones (algunas dependen de múltiples extracciones)
    transform_orders = BashOperator(
        task_id='transform_orders',
        bash_command='echo "⚙️ Transform orders (necesita orders + customers)"'
    )
    
    transform_products = BashOperator(
        task_id='transform_products',
        bash_command='echo "⚙️ Transform products"'
    )
    
    # Layer 3: Agregaciones
    aggregate_sales = BashOperator(
        task_id='aggregate_sales',
        bash_command='echo "📊 Sales aggregations (necesita orders + products)"'
    )
    
    aggregate_inventory = BashOperator(
        task_id='aggregate_inventory',
        bash_command='echo "📊 Inventory aggregations (necesita products)"'
    )
    
    # Layer 4: Reportes
    generate_report = BashOperator(
        task_id='generate_report',
        bash_command='echo "📄 Generando reporte final"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Dependencies complejas
    start >> [extract_orders, extract_customers, extract_products]
    
    # transform_orders necesita orders Y customers
    [extract_orders, extract_customers] >> transform_orders
    
    # transform_products solo necesita products
    extract_products >> transform_products
    
    # aggregate_sales necesita orders transformados Y products transformados
    [transform_orders, transform_products] >> aggregate_sales
    
    # aggregate_inventory solo necesita products transformados
    transform_products >> aggregate_inventory
    
    # Reporte final necesita ambas agregaciones
    [aggregate_sales, aggregate_inventory] >> generate_report
    
    generate_report >> end

dag.doc_md = """
# Task Relationships - Dependencias Complejas

Patrón común en data warehousing donde las transformaciones
dependen de múltiples fuentes.

## Visualización del flujo:

```
                    start
                      |
        +-------------+-------------+
        |             |             |
    orders       customers      products
        |             |             |
        +------+------+             |
               |                    |
        transform_orders      transform_products
               |                    |
               |          +---------+---------+
               |          |                   |
               +-----+----+             aggregate_inventory
                     |                         |
               aggregate_sales                 |
                     |                         |
                     +------------+------------+
                                  |
                            generate_report
                                  |
                                 end
```

## Cross-dependencies:

```python
# Tarea que depende de múltiples sources
[task_a, task_b] >> task_c

# Múltiples tareas dependen de una source
task_a >> [task_b, task_c]

# Cross: múltiples a múltiples
[task_a, task_b] >> [task_c, task_d]
# Equivalente a:
task_a >> task_c
task_a >> task_d
task_b >> task_c
task_b >> task_d
```

## Best practices:

1. **Claridad**: Agrupar dependencies por layer/stage
2. **Paralelismo**: Maximizar tareas independientes
3. **Checkpoints**: EmptyOperator entre stages para claridad visual
4. **Evitar cycles**: Airflow es un DAG (Directed Acyclic Graph)
"""
