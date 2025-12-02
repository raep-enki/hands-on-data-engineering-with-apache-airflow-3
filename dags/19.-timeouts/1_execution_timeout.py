"""
Timeouts - Execution Timeout Básico

Demuestra el uso de execution_timeout para limitar
el tiempo máximo que una tarea puede ejecutarse.

Si la tarea excede el timeout, Airflow la mata automáticamente.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='timeouts_basic_execution',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'timeouts']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Tarea normal (sin timeout explícito)
    normal_task = BashOperator(
        task_id='normal_task',
        bash_command='echo "✅ Tarea normal (sin timeout)"; sleep 2'
    )
    
    # Tarea con timeout corto (completará exitosamente)
    quick_task = BashOperator(
        task_id='quick_task_with_timeout',
        bash_command='echo "⚡ Tarea rápida"; sleep 1',
        execution_timeout=timedelta(seconds=5)  # 5 segundos suficientes
    )
    
    # Tarea que podría exceder timeout (simulación)
    potentially_slow = BashOperator(
        task_id='potentially_slow',
        bash_command='echo "🐌 Tarea que podría ser lenta"; sleep 2',
        execution_timeout=timedelta(seconds=10)
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> normal_task >> quick_task >> potentially_slow >> end

dag.doc_md = """
# Execution Timeout

**execution_timeout**: Tiempo máximo permitido para que una tarea se ejecute.

## Sintaxis:

```python
from datetime import timedelta

task = BashOperator(
    task_id='my_task',
    bash_command='long_running_process.sh',
    execution_timeout=timedelta(minutes=30)  # Máximo 30 minutos
)
```

## Comportamiento:

1. **Tarea completa antes del timeout**: Success ✅
2. **Tarea excede timeout**: 
   - Airflow envía señal para matar el proceso
   - Task marca como Failed ❌
   - Logs muestran: "Task exceeded execution_timeout"

## Cuándo usar:

- **Queries de base de datos**: Evitar queries que cuelgan
- **API calls**: Timeout en llamadas externas
- **Batch processing**: Limitar procesamiento de archivos grandes
- **ML training**: Evitar entrenamientos infinitos

## Valores típicos:

```python
# Queries rápidas
execution_timeout=timedelta(minutes=5)

# ETL moderado
execution_timeout=timedelta(minutes=30)

# Batch processing pesado
execution_timeout=timedelta(hours=2)

# ML training
execution_timeout=timedelta(hours=6)
```

## Diferencia con retry:

- **timeout**: Mata tarea que tarda mucho
- **retries**: Reintenta tarea que falló
- Pueden combinarse: timeout en cada intento
"""
