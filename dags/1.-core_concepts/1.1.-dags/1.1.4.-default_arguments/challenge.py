"""
# DESAFÍO: Pipeline de Procesamiento de Datos con Default Arguments y Overrides

## Objetivo
Crear un DAG que demuestre el uso efectivo de default_args y cómo tareas específicas
pueden sobrescribir estos valores predeterminados según sus necesidades particulares.

## Contexto del Negocio
Eres el Data Engineer responsable del pipeline ETL de una plataforma de e-commerce.
El pipeline procesa datos de ventas, inventario y usuarios, donde diferentes tareas
tienen requisitos distintos de reintentos, timeouts y notificaciones.

## Requisitos

### Configuración del DAG
- **DAG ID**: `ecommerce_etl_pipeline`
- **Schedule**: `0 2 * * *` (2 AM diario)
- **Start Date**: 2024-11-01
- **Catchup**: False
- **Tags**: `['challenge', 'core_concepts', 'dags', 'default_arguments']`
- **Description**: "Pipeline ETL de e-commerce con configuraciones específicas por tarea"

### Default Arguments (nivel DAG)
Configura estos valores predeterminados que heredarán todas las tareas:

- `owner`: 'equipo_data'
- `email`: ['data@ecommerce.com', 'ops@ecommerce.com']
- `email_on_failure`: True
- `email_on_retry`: False
- `retries`: 3
- `retry_delay`: timedelta(minutes=10)
- `execution_timeout`: timedelta(minutes=30)

### Tareas Requeridas

Implementa las siguientes tareas usando **BashOperator** y **EmptyOperator**:

#### Grupo 1: Extracción de Datos

**Tarea 1: `inicio`** (EmptyOperator)
- Marca el inicio del pipeline
- Usa defaults del DAG

**Tarea 2: `extraer_ventas`** (BashOperator)
- Comando: `echo "Extrayendo datos de ventas de API externa"`
- Usa defaults del DAG

**Tarea 3: `extraer_inventario`** (BashOperator)
- Comando: `echo "Extrayendo datos de inventario de base de datos"`
- **Override**: `retries=1` (sistema muy estable, pocas fallas)

**Tarea 4: `extraer_usuarios`** (BashOperator)
- Comando: `echo "Extrayendo datos de usuarios y comportamiento"`
- Usa defaults del DAG

#### Grupo 2: Validación

**Tarea 5: `validar_datos_criticos`** (BashOperator)
- Comando: `echo "Validando integridad de datos críticos del negocio"`
- **Override**: `retries=0` (si falla validación, no reintentar, alertar inmediato)
- **Override**: `email_on_failure=True, email_on_retry=True`

**Tarea 6: `validar_formatos`** (BashOperator)
- Comando: `echo "Validando formatos y esquemas de datos"`
- Usa defaults del DAG

#### Grupo 3: Transformaciones

**Tarea 7: `transformar_ventas`** (BashOperator)
- Comando: `echo "Aplicando lógica de negocio a datos de ventas"`
- Usa defaults del DAG

**Tarea 8: `calcular_metricas_heavy`** (BashOperator)
- Comando: `echo "Calculando métricas complejas y KPIs (proceso largo)"`
- **Override**: `execution_timeout=timedelta(hours=2)` (proceso puede tardar)
- **Override**: `retries=1` (muy intensivo, no reintentar mucho)

**Tarea 9: `enriquecer_datos`** (BashOperator)
- Comando: `echo "Enriqueciendo datos con información externa"`
- **Override**: `retries=5` (API externa inestable)
- **Override**: `retry_delay=timedelta(minutes=5)` (retry más rápido)

#### Grupo 4: Carga

**Tarea 10: `cargar_warehouse`** (BashOperator)
- Comando: `echo "Cargando datos procesados al data warehouse"`
- Usa defaults del DAG

**Tarea 11: `actualizar_cache`** (BashOperator)
- Comando: `echo "Actualizando cache de aplicación"`
- **Override**: `email_on_failure=False` (no crítico, no alertar)
- **Override**: `execution_timeout=timedelta(minutes=5)` (rápido)

**Tarea 12: `generar_reportes_ejecutivos`** (BashOperator)
- Comando: `echo "Generando reportes para stakeholders"`
- **Override**: `email=['data@ecommerce.com', 'ceo@ecommerce.com', 'cfo@ecommerce.com']`
- **Override**: `email_on_failure=True`

**Tarea 13: `fin`** (EmptyOperator)
- Marca el fin del pipeline
- Usa defaults del DAG

### Dependencias de Tareas

Implementa la siguiente estructura:

```
inicio
    ↓
[extraer_ventas, extraer_inventario, extraer_usuarios] (paralelo)
    ↓
[validar_datos_criticos, validar_formatos] (paralelo)
    ↓
[transformar_ventas, calcular_metricas_heavy, enriquecer_datos] (paralelo)
    ↓
cargar_warehouse
    ↓
[actualizar_cache, generar_reportes_ejecutivos] (paralelo)
    ↓
fin
```

## Restricciones

1. **Solo usar BashOperator y EmptyOperator**
2. **NO usar PythonOperator**
3. **Todos los BashOperator deben usar echo con mensajes descriptivos**
4. **Importar timedelta de datetime para configurar delays y timeouts**
5. **Mensajes en español, palabras reservadas en inglés**
6. **Un único DAG en el archivo `challenge.py`**

## Imports Necesarios

```python
import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
```

## Criterios de Éxito

- [ ] DAG creado con el dag_id `ecommerce_etl_pipeline`
- [ ] default_args configurados correctamente a nivel DAG
- [ ] 13 tareas implementadas con los task_ids especificados
- [ ] Tareas con defaults: inicio, extraer_ventas, extraer_usuarios, validar_formatos, transformar_ventas, cargar_warehouse, fin
- [ ] Tareas con overrides correctos:
  - [ ] `extraer_inventario`: retries=1
  - [ ] `validar_datos_criticos`: retries=0, email_on_failure=True, email_on_retry=True
  - [ ] `calcular_metricas_heavy`: execution_timeout=2 horas, retries=1
  - [ ] `enriquecer_datos`: retries=5, retry_delay=5 minutos
  - [ ] `actualizar_cache`: email_on_failure=False, execution_timeout=5 minutos
  - [ ] `generar_reportes_ejecutivos`: email sobrescrito con lista específica
- [ ] Dependencias implementadas según el diagrama
- [ ] Usa context manager style (`with DAG(...)`)

## Validación

Para verificar tu solución:
1. El DAG carga sin errores en la UI de Airflow
2. Inspecciona cada tarea en la UI y verifica en "Details" que:
   - Las tareas sin override muestran los valores de default_args
   - Las tareas con override muestran los valores específicos sobrescritos
3. La estructura de dependencias se ve correcta en Graph View
4. Ejecuta el DAG manualmente y verifica que todas las tareas completan

## Validación Específica de Overrides

Verifica en la UI que estas tareas muestren valores diferentes a los defaults:
- `extraer_inventario` debe mostrar 1 retry (no 3)
- `validar_datos_criticos` debe mostrar 0 retries
- `calcular_metricas_heavy` debe mostrar timeout de 2 horas (no 30 minutos)
- `enriquecer_datos` debe mostrar 5 retries y retry_delay de 5 minutos
- `actualizar_cache` debe mostrar email_on_failure=False

¡Buena suerte! Este desafío demuestra cómo usar default_args eficientemente mientras
mantienes flexibilidad para casos especiales.
"""

# TODO: Implementa el DAG según las especificaciones anteriores

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Implementa tu solución aquí...
