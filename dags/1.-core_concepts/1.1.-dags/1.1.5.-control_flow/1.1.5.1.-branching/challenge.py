"""
# DESAFÍO: Sistema de Procesamiento de Datos con Branching Inteligente

## Objetivo
Crear un DAG que use branching para procesar datos de manera diferente según
el volumen de datos detectado y la hora del día.

## Contexto del Negocio
Eres el Data Engineer de una plataforma de analytics que procesa logs de eventos.
El sistema debe decidir qué estrategia de procesamiento usar dependiendo de:
1. El volumen de datos esperado (simulado por día del mes)
2. La hora de ejecución (horario laboral vs noche)

## Requisitos

### Configuración del DAG
- **DAG ID**: `smart_data_processing`
- **Schedule**: `0 */4 * * *` (cada 4 horas)
- **Start Date**: 2024-01-01
- **Catchup**: False
- **Tags**: `['challenge', 'core_concepts', 'dags', 'control_flow', 'branching']`
- **Description**: "Pipeline de procesamiento con branching inteligente según volumen"

### Lógica de Branching

Implementa una función `choose_processing_strategy()` que:

1. **Obtiene la hora de ejecución** del context (`logical_date`)
2. **Obtiene el día del mes** del context
3. **Decide la estrategia** según estas reglas:

   **Regla 1: Volumen Muy Alto (días 1-5 del mes)**
   - Siempre usar: `procesamiento_distribuido`
   
   **Regla 2: Volumen Alto (días 6-15 del mes)**
   - Si hora entre 8 AM y 6 PM → `procesamiento_paralelo`
   - Si hora fuera de horario → `procesamiento_distribuido`
   
   **Regla 3: Volumen Medio (días 16-25 del mes)**
   - Si hora entre 8 AM y 6 PM → `procesamiento_estandar`
   - Si hora fuera de horario → `procesamiento_paralelo`
   
   **Regla 4: Volumen Bajo (días 26-31 del mes)**
   - Siempre usar: `procesamiento_estandar`

### Estructura del DAG

**Tareas Iniciales:**
1. `inicio` (EmptyOperator)
2. `verificar_origen` (BashOperator) - "Verificando disponibilidad de datos"
3. `preparar_staging` (BashOperator) - "Preparando área de staging"

**Branching:**
4. `seleccionar_estrategia` (BranchPythonOperator) - Implementa la lógica anterior

**Estrategias de Procesamiento (Ramas):**

**Rama 1: Procesamiento Estándar**
5. `procesamiento_estandar` (BashOperator) - "Procesando en modo estándar (single-threaded)"
6. `validar_resultados_estandar` (BashOperator) - "Validando resultados estándar"

**Rama 2: Procesamiento Paralelo**
7. `procesamiento_paralelo` (BashOperator) - "Procesando en modo paralelo (multi-threaded)"
8. `validar_resultados_paralelo` (BashOperator) - "Validando resultados paralelos"
9. `consolidar_paralelo` (BashOperator) - "Consolidando resultados de threads"

**Rama 3: Procesamiento Distribuido**
10. `procesamiento_distribuido` (BashOperator) - "Procesando en modo distribuido (cluster)"
11. `validar_resultados_distribuido` (BashOperator) - "Validando resultados distribuidos"
12. `consolidar_distribuido` (BashOperator) - "Consolidando resultados del cluster"

**Tareas Finales (se ejecutan siempre):**
13. `cargar_warehouse` (BashOperator) - "Cargando resultados al data warehouse"
14. `actualizar_metricas` (BashOperator) - "Actualizando métricas de procesamiento"
15. `fin` (EmptyOperator)

### Dependencias

```
inicio
    ↓
verificar_origen
    ↓
preparar_staging
    ↓
seleccionar_estrategia
    ↓
    ├── procesamiento_estandar >> validar_resultados_estandar
    │                                        ↓
    ├── procesamiento_paralelo >> validar_resultados_paralelo >> consolidar_paralelo
    │                                        ↓
    └── procesamiento_distribuido >> validar_resultados_distribuido >> consolidar_distribuido
                                             ↓
                                [Todas las ramas convergen]
                                             ↓
                                     cargar_warehouse
                                             ↓
                                    actualizar_metricas
                                             ↓
                                            fin
```

## Restricciones

1. **Solo usar BranchPythonOperator, BashOperator y EmptyOperator**
2. **NO usar @task.branch (TaskFlow API)**
3. **Implementar la función choose_processing_strategy() correctamente**
4. **Todos los BashOperator deben usar echo con mensajes descriptivos**
5. **Las tareas finales deben ejecutarse sin importar qué rama se tome**
6. **Un único DAG en el archivo `challenge.py`**

## Imports Necesarios

```python
import datetime

from airflow.sdk import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
```

## Función de Ayuda

Para obtener la hora del día:
```python
logical_date = context['logical_date']
hour = logical_date.hour  # 0-23
day = logical_date.day    # 1-31
```

## Criterios de Éxito

- [ ] DAG creado con el dag_id `smart_data_processing`
- [ ] 15 tareas implementadas según especificación
- [ ] Función `choose_processing_strategy()` implementada con lógica correcta
- [ ] Branch retorna el task_id correcto según las reglas
- [ ] Las 3 ramas (estrategias) están correctamente definidas
- [ ] Cada rama tiene su secuencia de tareas específica
- [ ] Las tareas finales convergen después de todas las ramas
- [ ] Dependencias implementadas según el diagrama
- [ ] Usa context manager style (`with DAG(...)`)

## Validación

Para verificar tu solución:
1. El DAG carga sin errores en la UI de Airflow
2. Inspecciona el Graph View para ver las 3 ramas
3. Ejecuta el DAG manualmente y observa qué rama se toma
4. Las tareas finales deben ejecutarse siempre

## Comportamiento Esperado por Fecha

- **Día 3, cualquier hora**: → `procesamiento_distribuido`
- **Día 10, 2 PM**: → `procesamiento_paralelo` (horario laboral)
- **Día 10, 10 PM**: → `procesamiento_distribuido` (fuera de horario)
- **Día 20, 9 AM**: → `procesamiento_estandar` (horario laboral)
- **Día 28, cualquier hora**: → `procesamiento_estandar` (volumen bajo)

¡Buena suerte! Este desafío simula decisiones reales de optimización de procesamiento.
"""

# TODO: Implementa el DAG según las especificaciones anteriores

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Implementa tu solución aquí...
