"""
Timeouts - Retry con Timeout

Demuestra cómo combinar execution_timeout con retries.
Cada retry tiene su propio timeout.

Útil para tareas que pueden fallar temporalmente.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='timeouts_with_retries',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'tasks', 'timeouts']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Tarea que puede fallar pero se reintenta
    api_call_with_timeout = BashOperator(
        task_id='api_call_with_timeout',
        bash_command='''
        echo "🌐 Llamando API externa..."
        sleep 2
        echo "✅ API respondió exitosamente"
        ''',
        execution_timeout=timedelta(seconds=10),  # Timeout por intento
        retries=3,                                 # 3 reintentos
        retry_delay=timedelta(seconds=30)          # Espera 30s entre reintentos
    )
    
    # Query de base de datos con timeout y retry
    db_query = BashOperator(
        task_id='db_query_with_retry',
        bash_command='''
        echo "🗄️ Ejecutando query en base de datos..."
        echo "SELECT * FROM large_table WHERE date = today"
        sleep 3
        echo "✅ Query completada"
        ''',
        execution_timeout=timedelta(minutes=5),
        retries=2,
        retry_delay=timedelta(minutes=1)
    )
    
    # Procesamiento con timeout más largo
    heavy_processing = BashOperator(
        task_id='heavy_processing',
        bash_command='''
        echo "⚙️ Procesamiento pesado iniciado..."
        sleep 5
        echo "✅ Procesamiento completado"
        ''',
        execution_timeout=timedelta(minutes=10),
        retries=1,
        retry_delay=timedelta(minutes=5)
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> api_call_with_timeout >> db_query >> heavy_processing >> end

dag.doc_md = """
# Timeout + Retries

Combinación de execution_timeout con retries para robustez.

## Configuración típica:

```python
task = BashOperator(
    task_id='resilient_task',
    bash_command='...',
    execution_timeout=timedelta(minutes=5),  # Por intento
    retries=3,                                # Número de reintentos
    retry_delay=timedelta(minutes=1),         # Espera entre reintentos
    retry_exponential_backoff=True            # Backoff exponencial
)
```

## Escenarios de ejecución:

### Escenario 1: Success en primer intento
```
Intento 1: Completa en 3 min → Success ✅
(No hay reintentos)
```

### Escenario 2: Timeout en primer intento, success en segundo
```
Intento 1: Timeout después de 5 min → Failed ⏱️
Espera: 1 minuto
Intento 2: Completa en 3 min → Success ✅
```

### Escenario 3: Timeouts en todos los intentos
```
Intento 1: Timeout después de 5 min → Failed ⏱️
Espera: 1 minuto
Intento 2: Timeout después de 5 min → Failed ⏱️
Espera: 1 minuto
Intento 3: Timeout después de 5 min → Failed ⏱️
Espera: 1 minuto
Intento 4 (último): Timeout después de 5 min → Failed ❌
Task marca como Failed definitivamente
```

## Retry exponential backoff:

```python
task = BashOperator(
    task_id='smart_retry',
    retries=5,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10)
)

# Delays:
# Retry 1: 30 segundos
# Retry 2: 60 segundos (2x)
# Retry 3: 120 segundos (2x)
# Retry 4: 240 segundos (2x)
# Retry 5: 480 segundos → limitado a max_retry_delay (600s)
```

## Casos de uso por tipo de tarea:

### API Calls
```python
# APIs pueden tener problemas temporales
execution_timeout=timedelta(seconds=30)
retries=5
retry_delay=timedelta(seconds=10)
retry_exponential_backoff=True
```

### Database Queries
```python
# DBs pueden estar temporalmente lentas
execution_timeout=timedelta(minutes=5)
retries=3
retry_delay=timedelta(minutes=1)
```

### File Processing
```python
# Archivos grandes pueden variar en tiempo
execution_timeout=timedelta(hours=1)
retries=2
retry_delay=timedelta(minutes=10)
```

### HTTP Downloads
```python
# Network puede fallar temporalmente
execution_timeout=timedelta(minutes=10)
retries=4
retry_delay=timedelta(seconds=30)
retry_exponential_backoff=True
```

## Monitoring de retries:

En los logs verás:
```
[2024-01-15 10:00:00] Task attempt 1 starting
[2024-01-15 10:05:00] ERROR - Task exceeded execution_timeout
[2024-01-15 10:06:00] INFO - Retrying task (attempt 2 of 4)
[2024-01-15 10:08:00] INFO - Task completed successfully
```

## Best practices:

1. **Timeout razonable**: Basado en comportamiento histórico + margen
2. **Retries limitados**: No más de 5 para evitar loops
3. **Exponential backoff**: Para problemas de congestión
4. **Max retry delay**: Limitar espera máxima
5. **Alertas**: Notificar cuando se agotan reintentos
"""
