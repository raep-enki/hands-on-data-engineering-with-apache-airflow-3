"""
Timeouts - Sensor con Timeout y Poke Interval

Demuestra el uso de timeout y poke_interval en sensores.
Los sensores revisan una condición cada X segundos hasta timeout.

Útil para esperar archivos, datos, o condiciones externas.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.bash import BashSensor

with DAG(
    dag_id='timeouts_sensors',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'timeouts']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Sensor que espera archivo (simulado)
    # Revisa cada 30 segundos, timeout después de 5 minutos
    wait_for_file = BashSensor(
        task_id='wait_for_file',
        bash_command='echo "📁 Revisando si existe archivo..."; exit 0',
        poke_interval=30,  # Revisar cada 30 segundos
        timeout=300,       # Timeout después de 5 minutos (300 segundos)
        mode='poke'        # Modo: 'poke' (ocupar slot) o 'reschedule' (liberar slot)
    )
    
    # Sensor con timeout más largo y menos frecuente
    wait_for_data = BashSensor(
        task_id='wait_for_data',
        bash_command='echo "🗄️ Revisando si hay datos nuevos..."; exit 0',
        poke_interval=120,  # Revisar cada 2 minutos
        timeout=1800,       # Timeout después de 30 minutos
        mode='poke'
    )
    
    # Sensor con reschedule mode (libera slot mientras espera)
    wait_for_external_system = BashSensor(
        task_id='wait_for_external_system',
        bash_command='echo "🌐 Revisando sistema externo..."; exit 0',
        poke_interval=300,   # Revisar cada 5 minutos
        timeout=3600,        # Timeout después de 1 hora
        mode='reschedule'    # Libera slot entre pokes
    )
    
    # Procesamiento después de que sensores completen
    process_data = BashOperator(
        task_id='process_data',
        bash_command='echo "⚙️ Procesando datos..."; sleep 2'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> wait_for_file >> wait_for_data >> wait_for_external_system >> process_data >> end

dag.doc_md = """
# Sensor Timeout vs Poke Interval

Sensores tienen configuración de timeout diferente a tareas normales.

## Configuración de sensores:

```python
from airflow.sensors.filesystem import FileSensor

sensor = FileSensor(
    task_id='wait_for_file',
    filepath='/data/input.csv',
    poke_interval=60,  # Revisar cada 60 segundos
    timeout=3600,      # Fallar después de 1 hora (3600 segundos)
    mode='poke'        # o 'reschedule'
)
```

## Parámetros clave:

### poke_interval
- **Qué hace**: Tiempo entre revisiones
- **Unidad**: Segundos
- **Default**: 60 segundos
- **Ejemplo**: `poke_interval=30` → revisa cada 30 segundos

### timeout
- **Qué hace**: Tiempo total máximo de espera
- **Unidad**: Segundos (NOT timedelta)
- **Default**: 604800 segundos (7 días)
- **Ejemplo**: `timeout=1800` → falla después de 30 minutos

### mode
- **poke**: Ocupa slot de worker mientras espera
- **reschedule**: Libera slot entre pokes (mejor para timeouts largos)

## Diferencia con execution_timeout:

```python
# ❌ INCORRECTO para sensores
sensor = FileSensor(
    task_id='wait',
    execution_timeout=timedelta(minutes=30)  # No usar esto
)

# ✅ CORRECTO para sensores
sensor = FileSensor(
    task_id='wait',
    timeout=1800  # 30 minutos en segundos
)
```

## Escenarios de ejecución:

### Escenario 1: Condición cumplida rápido
```
00:00 - Sensor inicia, revisa condición → False
00:01 - Poke (después de poke_interval) → False
00:02 - Poke → True ✅
Sensor marca como Success
```

### Escenario 2: Timeout alcanzado
```
00:00 - Sensor inicia → False
00:01 - Poke → False
00:02 - Poke → False
...
00:30 - Timeout (1800s) alcanzado
Sensor marca como Failed ❌
```

## Estrategia de poke_interval:

### Archivos locales (rápidos de revisar)
```python
poke_interval=15  # Cada 15 segundos
timeout=300       # 5 minutos total
```

### Queries de base de datos (moderados)
```python
poke_interval=60   # Cada minuto
timeout=1800       # 30 minutos total
```

### APIs externas (lentos, limitación de rate)
```python
poke_interval=300  # Cada 5 minutos
timeout=3600       # 1 hora total
```

### Batch jobs nocturnos (muy lentos)
```python
poke_interval=600  # Cada 10 minutos
timeout=21600      # 6 horas total
```

## Modo: poke vs reschedule

### Poke Mode (default)
```python
mode='poke'
```
**Ventajas**:
- Respuesta inmediata cuando condición cumple
- Simple de entender

**Desventajas**:
- Ocupa slot de worker
- No escala para muchos sensores

**Cuándo usar**: poke_interval corto (< 5 min)

### Reschedule Mode
```python
mode='reschedule'
```
**Ventajas**:
- Libera slot entre pokes
- Escala para muchos sensores
- Mejor para timeouts largos

**Desventajas**:
- Overhead de rescheduling
- Latencia adicional

**Cuándo usar**: poke_interval largo (> 5 min)

## Ejemplo completo:

```python
from airflow.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.bash import BashOperator

with DAG(...):
    
    # Espera archivo con timeout corto
    wait_input = FileSensor(
        task_id='wait_for_input',
        filepath='/data/input.csv',
        poke_interval=30,   # Cada 30 segundos
        timeout=600,        # 10 minutos
        mode='poke'
    )
    
    # Espera sistema externo con timeout largo
    wait_external = BashSensor(
        task_id='wait_for_external',
        bash_command='curl -f https://api.example.com/ready',
        poke_interval=300,  # Cada 5 minutos
        timeout=7200,       # 2 horas
        mode='reschedule'   # Libera slot
    )
    
    # Procesamiento
    process = BashOperator(
        task_id='process',
        bash_command='python process.py',
        execution_timeout=timedelta(minutes=30)  # Timeout normal
    )
    
    wait_input >> wait_external >> process
```

## Monitoring:

En Task Instance verás:
```
Start Time: 2024-01-15 10:00:00
Duration: 00:15:30
Poke Count: 31
Status: Success
```

## Best practices:

1. **Timeout razonable**: Basado en SLA del proceso upstream
2. **Poke interval conservador**: No saturar sistemas externos
3. **Reschedule para largos**: Liberar recursos
4. **Alertas**: Notificar si sensor cerca de timeout
5. **Soft fail**: Considerar `soft_fail=True` para timeouts no críticos

```python
# Soft fail: continúa pipeline incluso si timeout
sensor = FileSensor(
    task_id='optional_file',
    filepath='/data/optional.csv',
    timeout=300,
    soft_fail=True  # No falla DAG si timeout
)
```
"""
