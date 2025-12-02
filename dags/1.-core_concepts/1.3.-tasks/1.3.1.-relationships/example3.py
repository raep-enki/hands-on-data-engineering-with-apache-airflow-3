"""
Task Relationships - Dependencias Upstream

Demuestra el uso del operador << (bitshift left)
para definir dependencias upstream (hacia atrás).

<< es el inverso de >>
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_relationships_upstream',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'tasks', 'relationships']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract = BashOperator(
        task_id='extract',
        bash_command='echo "📥 Extract"'
    )
    
    transform = BashOperator(
        task_id='transform',
        bash_command='echo "⚙️ Transform"'
    )
    
    load = BashOperator(
        task_id='load',
        bash_command='echo "📤 Load"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Usando << (upstream)
    # Se lee: "transform necesita extract antes"
    transform << extract
    
    # Equivale a: extract >> transform
    
    # Combinando >> y <<
    start >> extract
    load << transform  # transform >> load
    end << load        # load >> end

dag.doc_md = """
# Task Relationships - Upstream

**Operador <<** (bitshift left): Define que la tarea de la derecha
debe completarse antes que la tarea de la izquierda.

## Comparación >> vs <<:

```python
# Downstream (>>)
task_a >> task_b  # "A va antes de B"

# Upstream (<<)
task_b << task_a  # "B necesita A antes"

# Ambos son equivalentes
```

## Cuándo usar cada uno:

### Usar >> (recomendado):
```python
# Flujo natural de lectura
start >> extract >> transform >> load >> end
```

### Usar <<:
```python
# Cuando defines la tarea después de sus dependencias
# o cuando el contexto lo hace más legible
final_report << [analysis_a, analysis_b, analysis_c]
```

## Métodos alternativos:

```python
# set_downstream()
task_a.set_downstream(task_b)

# set_upstream()
task_b.set_upstream(task_a)

# Recomendación: Usar >> y << por ser más conciso
```
"""
