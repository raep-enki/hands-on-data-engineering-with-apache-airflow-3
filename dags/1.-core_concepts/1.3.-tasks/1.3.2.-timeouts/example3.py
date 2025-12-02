"""
Timeouts - Default Args para Timeout

Demuestra cómo establecer execution_timeout a nivel de DAG
mediante default_args. Todas las tareas heredan el timeout.

Tareas individuales pueden sobrescribir el default.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Default args aplicados a todas las tareas
default_args = {
    'execution_timeout': timedelta(minutes=5),  # Timeout default
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id='timeouts_default_args',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,  # Aplicar defaults
    tags=['example', 'core_concepts', 'tasks', 'timeouts']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Hereda timeout de 5 minutos
    task_with_default = BashOperator(
        task_id='task_with_default_timeout',
        bash_command='echo "⏱️ Timeout default: 5 minutos"; sleep 2'
    )
    
    # Sobrescribe timeout a 30 segundos
    task_with_short_timeout = BashOperator(
        task_id='task_with_short_timeout',
        bash_command='echo "⚡ Timeout sobrescrito: 30 segundos"; sleep 1',
        execution_timeout=timedelta(seconds=30)  # Override
    )
    
    # Sobrescribe timeout a 15 minutos
    task_with_long_timeout = BashOperator(
        task_id='task_with_long_timeout',
        bash_command='echo "🐢 Timeout sobrescrito: 15 minutos"; sleep 3',
        execution_timeout=timedelta(minutes=15)  # Override
    )
    
    # Sin timeout (None sobrescribe default)
    task_no_timeout = BashOperator(
        task_id='task_no_timeout',
        bash_command='echo "∞ Sin timeout"; sleep 1',
        execution_timeout=None  # Elimina timeout
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> [task_with_default, task_with_short_timeout, 
              task_with_long_timeout, task_no_timeout] >> end

dag.doc_md = """
# Default Args para Timeouts

Centralizar configuración de timeouts en default_args.

## Configuración:

```python
default_args = {
    'execution_timeout': timedelta(minutes=10),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1)
}

with DAG(
    dag_id='my_dag',
    default_args=default_args,  # Aplicado a todas las tareas
    ...
):
    # Todas las tareas heredan timeout de 10 minutos
    task1 = BashOperator(...)
    task2 = BashOperator(...)
```

## Herencia y override:

```python
# Default
default_args = {
    'execution_timeout': timedelta(minutes=5)
}

with DAG(..., default_args=default_args):
    
    # Hereda: 5 minutos
    normal_task = BashOperator(
        task_id='normal',
        bash_command='...'
    )
    
    # Override a 30 segundos
    quick_task = BashOperator(
        task_id='quick',
        bash_command='...',
        execution_timeout=timedelta(seconds=30)
    )
    
    # Override a 1 hora
    long_task = BashOperator(
        task_id='long',
        bash_command='...',
        execution_timeout=timedelta(hours=1)
    )
    
    # Sin timeout
    unlimited_task = BashOperator(
        task_id='unlimited',
        bash_command='...',
        execution_timeout=None
    )
```

## Estrategia de defaults por tipo de DAG:

### ETL ligero (APIs, queries simples)
```python
default_args = {
    'execution_timeout': timedelta(minutes=10),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}
```

### ETL moderado (procesamiento de archivos)
```python
default_args = {
    'execution_timeout': timedelta(minutes=30),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}
```

### ETL pesado (big data, ML)
```python
default_args = {
    'execution_timeout': timedelta(hours=2),
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}
```

### Batch nocturno (ventana amplia)
```python
default_args = {
    'execution_timeout': timedelta(hours=6),
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}
```

## Ventajas:

1. **Consistencia**: Todas las tareas tienen mismo timeout
2. **Centralizado**: Un lugar para cambiar configuración
3. **Sobrescritura selectiva**: Tareas especiales pueden override
4. **Mantenimiento**: Fácil ajustar valores

## Patrón común:

```python
# Defaults conservadores
default_args = {
    'execution_timeout': timedelta(minutes=15),
    'retries': 2
}

with DAG(..., default_args=default_args):
    
    # 90% de tareas heredan defaults
    extract_users = BashOperator(...)
    extract_orders = BashOperator(...)
    validate = BashOperator(...)
    
    # 10% necesitan override (tareas pesadas)
    ml_training = BashOperator(
        task_id='ml_training',
        bash_command='python train_model.py',
        execution_timeout=timedelta(hours=4)  # Override
    )
    
    large_export = BashOperator(
        task_id='large_export',
        bash_command='python export_to_s3.py',
        execution_timeout=timedelta(hours=1)  # Override
    )
```

## Monitoreo:

En la UI de Airflow, puedes ver:
- **Task Instance Details**: Timeout configurado
- **Task Duration**: Tiempo real vs timeout
- **Retry History**: Cuántos intentos con timeout

## Tips:

1. Empezar con timeout conservador (más tiempo)
2. Monitorear duración real de tareas
3. Ajustar timeouts basándose en histórico
4. Considerar variabilidad (picos de carga)
5. Alertar si tareas se acercan al timeout
"""
