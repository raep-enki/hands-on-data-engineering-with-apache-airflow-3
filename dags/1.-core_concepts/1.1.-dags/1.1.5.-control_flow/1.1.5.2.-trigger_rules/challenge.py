"""
# DESAFÍO: Sistema de Procesamiento Paralelo con Manejo de Fallos

## Objetivo
Crear un DAG que procese datos de múltiples fuentes en paralelo y use trigger rules
para manejar diferentes escenarios: éxito parcial, fallos y recuperación.

## Contexto del Negocio
Eres el Data Engineer de una empresa que procesa datos de 4 fuentes diferentes:
- Base de datos interna
- API externa 1
- API externa 2
- Archivos en S3

El sistema debe ser resiliente: si algunas fuentes fallan, el procesamiento debe
continuar con los datos disponibles, pero también debe alertar y intentar recuperación.

## Requisitos

### Configuración del DAG
- **DAG ID**: `resilient_data_pipeline`
- **Schedule**: `0 2 * * *` (2 AM diario)
- **Start Date**: 2024-01-01
- **Catchup**: False
- **Tags**: `['challenge', 'core_concepts', 'dags', 'control_flow', 'trigger_rules']`
- **Description**: "Pipeline resiliente con trigger rules para manejo de fallos"

### Estructura del DAG

**Fase 1: Extracción (4 tareas en paralelo)**
1. `inicio` (EmptyOperator)
2. `extraer_database` (BashOperator) - "Extrayendo de base de datos interna"
3. `extraer_api_1` (BashOperator) - "Extrayendo de API externa 1"
4. `extraer_api_2` (BashOperator) - "Extrayendo de API externa 2"
5. `extraer_s3` (BashOperator) - "Extrayendo archivos de S3"

**Fase 2: Alertas y Validación**
6. `alerta_primer_fallo` (BashOperator) - "🚨 Alerta: al menos una fuente falló"
   - **trigger_rule**: `ONE_FAILED`
   
7. `alerta_fallo_critico` (BashOperator) - "🔴 CRÍTICO: Todas las fuentes fallaron"
   - **trigger_rule**: `ALL_FAILED`

8. `validar_datos_disponibles` (BashOperator) - "Validando datos extraídos exitosamente"
   - **trigger_rule**: `ONE_SUCCESS`

**Fase 3: Procesamiento Condicional**
9. `procesar_datos_completos` (BashOperator) - "Procesando dataset completo (todas las fuentes)"
   - **trigger_rule**: `ALL_SUCCESS`
   
10. `procesar_datos_parciales` (BashOperator) - "Procesando con datos parciales"
    - **trigger_rule**: `NONE_FAILED_MIN_ONE_SUCCESS`

**Fase 4: Transformaciones (después de procesamiento)**
11. `transformar_datos` (BashOperator) - "Aplicando transformaciones"
    - **trigger_rule**: `NONE_FAILED`
    
12. `calcular_metricas` (BashOperator) - "Calculando métricas de calidad"
    - **trigger_rule**: `NONE_FAILED`

**Fase 5: Carga y Finalización**
13. `cargar_warehouse` (BashOperator) - "Cargando datos al warehouse"
    - **trigger_rule**: `NONE_FAILED`

14. `generar_reporte` (BashOperator) - "Generando reporte de ejecución"
    - **trigger_rule**: `ALL_DONE`
    
15. `limpiar_staging` (BashOperator) - "Limpiando área de staging"
    - **trigger_rule**: `ALL_DONE`
    
16. `fin` (EmptyOperator)
    - **trigger_rule**: `ALL_DONE`

### Dependencias

```
inicio
    ↓
[extraer_database, extraer_api_1, extraer_api_2, extraer_s3] (paralelo)
    ↓
    ├── alerta_primer_fallo (trigger: ONE_FAILED)
    ├── alerta_fallo_critico (trigger: ALL_FAILED)
    ├── validar_datos_disponibles (trigger: ONE_SUCCESS)
    ├── procesar_datos_completos (trigger: ALL_SUCCESS)
    └── procesar_datos_parciales (trigger: NONE_FAILED_MIN_ONE_SUCCESS)
    ↓
[transformar_datos, calcular_metricas] (paralelo, trigger: NONE_FAILED)
    ↓
cargar_warehouse (trigger: NONE_FAILED)
    ↓
[generar_reporte, limpiar_staging] (paralelo, trigger: ALL_DONE)
    ↓
fin (trigger: ALL_DONE)
```

## Restricciones

1. **Solo usar BashOperator y EmptyOperator**
2. **NO usar PythonOperator ni @task decorators**
3. **Todos los BashOperator deben usar echo con mensajes descriptivos**
4. **Cada tarea debe tener el trigger_rule especificado**
5. **Las tareas de limpieza SIEMPRE deben ejecutarse**
6. **Un único DAG en el archivo `challenge.py`**

## Imports Necesarios

```python
import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
```

## Trigger Rules a Usar

- `TriggerRule.ONE_FAILED` - Para alertas tempranas
- `TriggerRule.ALL_FAILED` - Para alertas críticas
- `TriggerRule.ONE_SUCCESS` - Para validación con al menos un éxito
- `TriggerRule.ALL_SUCCESS` - Para procesamiento completo
- `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` - Para procesamiento parcial
- `TriggerRule.NONE_FAILED` - Para transformaciones
- `TriggerRule.ALL_DONE` - Para tareas de finalización

## Criterios de Éxito

- [ ] DAG creado con el dag_id `resilient_data_pipeline`
- [ ] 16 tareas implementadas según especificación
- [ ] Todas las trigger_rules configuradas correctamente
- [ ] 4 tareas de extracción en paralelo
- [ ] 5 tareas de alertas/validación con diferentes trigger rules
- [ ] 2 tareas de procesamiento (completo vs parcial)
- [ ] 2 tareas de transformación
- [ ] 3 tareas de finalización que siempre se ejecutan
- [ ] Dependencias implementadas según el diagrama
- [ ] Usa context manager style (`with DAG(...)`)

## Validación

Para verificar tu solución:
1. El DAG carga sin errores en la UI de Airflow
2. Inspecciona el Graph View para verificar todas las conexiones
3. Simula diferentes escenarios cambiando comandos bash:
   - **Todas exitosas**: Dejar todos con echo
   - **Una falla**: Cambiar una extracción a `exit 1`
   - **Todas fallan**: Cambiar todas las extracciones a `exit 1`
4. Verifica que las tareas se ejecuten según su trigger_rule
5. Las tareas de limpieza SIEMPRE deben ejecutarse

## Escenarios de Prueba

### Escenario 1: Todas las fuentes exitosas
- Deben ejecutarse: todas las extracciones, validar, procesar_completo, transformar, cargar, reporte, limpiar
- NO deben ejecutarse: alerta_primer_fallo, alerta_fallo_critico, procesar_parcial

### Escenario 2: Una fuente falla
- Deben ejecutarse: 3 extracciones exitosas, alerta_primer_fallo, validar, procesar_parcial, transformar, cargar
- NO deben ejecutarse: procesar_completo
- SIEMPRE ejecutan: reporte, limpiar, fin

### Escenario 3: Todas las fuentes fallan
- Deben ejecutarse: alerta_primer_fallo, alerta_fallo_critico
- NO deben ejecutarse: validar, procesar_completo, procesar_parcial, transformar, cargar
- SIEMPRE ejecutan: reporte, limpiar, fin

¡Buena suerte! Este desafío simula un sistema real de data engineering resiliente.
"""

# TODO: Implementa el DAG según las especificaciones anteriores

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Implementa tu solución aquí...
