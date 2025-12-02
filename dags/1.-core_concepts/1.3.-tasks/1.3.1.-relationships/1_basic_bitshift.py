"""
Task Relationships - Dependencias Básicas

Demuestra cómo crear dependencias simples entre tareas
usando el operador >> (bitshift right).

En Airflow, las dependencias definen el orden de ejecución.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_relationships_basic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'tasks', 'relationships']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo "Ejecutando Task A"'
    )
    
    task_b = BashOperator(
        task_id='task_b',
        bash_command='echo "Ejecutando Task B (después de A)"'
    )
    
    task_c = BashOperator(
        task_id='task_c',
        bash_command='echo "Ejecutando Task C (después de B)"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Dependencias lineales: start -> A -> B -> C -> end
    start >> task_a >> task_b >> task_c >> end

dag.doc_md = """
# Task Relationships - Dependencias Básicas

**Operador >>** (bitshift right): Define que la tarea de la izquierda
debe completarse antes que la tarea de la derecha.

## Sintaxis:

```python
# Forma básica
task_a >> task_b  # A debe ejecutarse antes que B

# Encadenado
task_a >> task_b >> task_c  # A -> B -> C

# Equivalente a
task_a.set_downstream(task_b)
task_b.set_downstream(task_c)
```

## Orden de ejecución:
1. start
2. task_a (espera que start termine)
3. task_b (espera que task_a termine)
4. task_c (espera que task_b termine)
5. end (espera que task_c termine)
"""
