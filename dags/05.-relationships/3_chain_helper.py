"""
Task Relationships - Chain Helper

Demuestra el uso de chain() para definir dependencias
lineales de forma más concisa.

Útil para pipelines con muchas etapas secuenciales.
"""

import datetime

from airflow.sdk import DAG, chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_relationships_chain',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'relationships']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Pipeline lineal largo
    stage_1 = BashOperator(task_id='validate_schema', bash_command='echo "✅ Schema"')
    stage_2 = BashOperator(task_id='extract_source', bash_command='echo "📥 Extract"')
    stage_3 = BashOperator(task_id='clean_data', bash_command='echo "🧹 Clean"')
    stage_4 = BashOperator(task_id='deduplicate', bash_command='echo "🔍 Dedup"')
    stage_5 = BashOperator(task_id='transform', bash_command='echo "⚙️ Transform"')
    stage_6 = BashOperator(task_id='enrich', bash_command='echo "✨ Enrich"')
    stage_7 = BashOperator(task_id='validate_quality', bash_command='echo "✅ Quality"')
    stage_8 = BashOperator(task_id='load_staging', bash_command='echo "📤 Staging"')
    stage_9 = BashOperator(task_id='load_production', bash_command='echo "📤 Production"')
    stage_10 = BashOperator(task_id='update_metadata', bash_command='echo "📝 Metadata"')
    
    end = EmptyOperator(task_id='end')
    
    # Usando chain() para pipeline lineal
    chain(
        start,
        stage_1,
        stage_2,
        stage_3,
        stage_4,
        stage_5,
        stage_6,
        stage_7,
        stage_8,
        stage_9,
        stage_10,
        end
    )
    
    # Equivalente a:
    # start >> stage_1 >> stage_2 >> ... >> stage_10 >> end

dag.doc_md = """
# Task Relationships - Chain Helper

**chain()**: Función helper para crear dependencias lineales
de múltiples tareas de forma más legible.

## Sintaxis:

```python
from airflow.sdk import chain

# Sin chain (verbose)
task_1 >> task_2 >> task_3 >> task_4 >> task_5

# Con chain (más limpio)
chain(task_1, task_2, task_3, task_4, task_5)
```

## Ventajas:

1. **Legibilidad**: Especialmente con muchas tareas
2. **Vertical listing**: Fácil de ver etapas del pipeline
3. **Menos >>**: Evita líneas largas

## Chain con listas (paralelismo):

```python
# Soporta fan-out/fan-in
chain(
    start,
    [extract_a, extract_b, extract_c],  # Paralelo
    transform,
    [load_a, load_b],  # Paralelo
    end
)

# Equivalente a:
start >> [extract_a, extract_b, extract_c]
[extract_a, extract_b, extract_c] >> transform
transform >> [load_a, load_b]
[load_a, load_b] >> end
```

## Cuándo usar chain():

- ✅ Pipelines lineales largos (5+ etapas)
- ✅ Combinar con listas para fan-out/fan-in
- ❌ Dependencias complejas (usar >> explícito)
- ❌ Solo 2-3 tareas (>> es más conciso)
"""
