"""
Task Relationships - Dependencias en Paralelo

Demuestra cómo ejecutar múltiples tareas en paralelo
y luego convergir en una tarea final.

Pattern: Fan-out / Fan-in
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_relationships_parallel',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'tasks', 'relationships']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Tareas en paralelo (fan-out)
    extract_sales = BashOperator(
        task_id='extract_sales',
        bash_command='echo "📥 Extrayendo sales"'
    )
    
    extract_customers = BashOperator(
        task_id='extract_customers',
        bash_command='echo "📥 Extrayendo customers"'
    )
    
    extract_products = BashOperator(
        task_id='extract_products',
        bash_command='echo "📥 Extrayendo products"'
    )
    
    # Convergencia (fan-in)
    join_data = BashOperator(
        task_id='join_data',
        bash_command='echo "🔗 Uniendo todas las fuentes"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Fan-out: start diverge a 3 tareas paralelas
    start >> [extract_sales, extract_customers, extract_products]
    
    # Fan-in: 3 tareas convergen en join_data
    [extract_sales, extract_customers, extract_products] >> join_data
    
    join_data >> end

dag.doc_md = """
# Task Relationships - Paralelo

**Pattern Fan-out/Fan-in**: Una tarea inicia múltiples tareas en paralelo,
y luego múltiples tareas convergen en una.

## Sintaxis para paralelismo:

```python
# Fan-out: Una tarea a múltiples
start >> [task_a, task_b, task_c]

# Equivalente a:
start >> task_a
start >> task_b
start >> task_c

# Fan-in: Múltiples tareas a una
[task_a, task_b, task_c] >> end

# Equivalente a:
task_a >> end
task_b >> end
task_c >> end
```

## Orden de ejecución:
1. start
2. extract_sales, extract_customers, extract_products (en paralelo)
3. join_data (espera que las 3 terminen)
4. end
"""
