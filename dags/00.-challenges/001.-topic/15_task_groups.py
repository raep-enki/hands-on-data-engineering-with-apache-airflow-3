"""
Challenge: Pipeline Multi-Región Que Se Ve Como Spaghetti

Tu empresa opera en 4 regiones (North America, South America, Europe, Asia Pacific) y el pipeline ETL
tiene tareas para cada una. El problema: el grafo visual del DAG es un desastre con 40+ tareas
desordenadas y nadie puede ver la estructura lógica. Es imposible encontrar dónde falla algo.

Tu lead te pide organizarlo usando `@task_group`: agrupar tareas de cada región en bloques visuales
colapsables. También agrupar validaciones iniciales juntas, y quality checks finales juntos.
Es como organizar código en funciones en vez de tener 500 líneas sueltas.

**Jerarquía visual deseada:**

```
start → [Initial Validations] → [Process NA] →
                             → [Process SA] → [Aggregate All] → [Quality Checks] → end
                             → [Process EU] →
                             → [Process APAC] →
```

Cada task group se colapsa/expande en la UI para explorar detalles.

**El flujo con task groups:**

`start` (EmptyOperator) >>

**Task Group 1: `initial_validations`**
Agrupa todas las validaciones iniciales:
- `check_source_systems` (BashOperator - verifica que todos los sistemas upstream estén up)
- `validate_credentials` (BashOperator - verifica conexiones a DBs de cada región)
- `check_disk_space` (BashOperator - verifica espacio suficiente para procesar)

**Task Groups 2-5: Una por cada región (en paralelo)**

Cada región tiene su task group con tareas internas idénticas:

`@task_group` **`process_north_america`:**
- `extract_na_data` (BashOperator - extrae desde US East + US West)
- `transform_na_data` (BashOperator - aplica reglas de negocio de NA)
- `validate_na_quality` (BashOperator - verifica data quality específica de NA)
- `load_na_to_staging` (BashOperator - carga a staging table NA)

`@task_group` **`process_south_america`:**
- `extract_sa_data` (BashOperator - extrae desde Brasil + Argentina + Chile)
- `transform_sa_data` (BashOperator - aplica reglas de negocio de SA, conversiones de moneda)
- `validate_sa_quality` (BashOperator - verifica data quality)
- `load_sa_to_staging` (BashOperator - carga a staging table SA)

`@task_group` **`process_europe`:**
- `extract_eu_data` (BashOperator - extrae desde UK + Germany + France)
- `transform_eu_data` (BashOperator - aplica GDPR compliance, reglas de UE)
- `validate_eu_quality` (BashOperator - verifica data quality con estándares EU)
- `load_eu_to_staging` (BashOperator - carga a staging table EU)

`@task_group` **`process_asia_pacific`:**
- `extract_apac_data` (BashOperator - extrae desde Japan + Australia + Singapore)
- `transform_apac_data` (BashOperator - aplica reglas de negocio de APAC)
- `validate_apac_quality` (BashOperator - verifica data quality)
- `load_apac_to_staging` (BashOperator - carga a staging table APAC)

**Task Group 6: `aggregate_all_regions`**
Consolida datos de todas las regiones:
- `union_all_staging_tables` (BashOperator - UNION de 4 staging tables)
- `deduplicate_global` (BashOperator - elimina duplicados cross-region)
- `calculate_global_metrics` (BashOperator - KPIs globales: revenue total, customers activos)
- `load_to_production` (BashOperator - carga a tabla final de producción)

**Task Group 7: `quality_checks_final`**
Validaciones finales antes de terminar:
- `row_count_validation` (BashOperator - verifica count esperado vs actual)
- `data_freshness_check` (BashOperator - verifica que datos son del día correcto)
- `referential_integrity` (BashOperator - verifica foreign keys válidos)
- `send_quality_report` (BashOperator - envía reporte de calidad a equipo)

>> `end` (EmptyOperator)

**Dependencias entre task groups:**

```python
initial_validations >> [process_na, process_sa, process_eu, process_apac] >> aggregate_all >> quality_checks >> end
```

Los 4 task groups de regiones corren en paralelo (máximo throughput).

**Beneficios visuales:**
- Grafo de alto nivel: 7 bloques en vez de 40+ tareas
- Cada task group se puede expandir/colapsar para ver detalles
- Fácil identificar en qué región falló algo
- Estructura lógica clara: validar → procesar → agregar → validar

**Pattern de task group:**
```python
@task_group(group_id='process_north_america')
def process_na():
    extract = BashOperator(task_id='extract_na_data', ...)
    transform = BashOperator(task_id='transform_na_data', ...)
    validate = BashOperator(task_id='validate_na_quality', ...)
    load = BashOperator(task_id='load_na_to_staging', ...)
    
    extract >> transform >> validate >> load
```

**Configuración técnica:**
- DAG ID: `multi_region_etl`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'task_groups', 'multi_region']`
- 7 task groups, 25+ tareas totales organizadas jerárquicamente
"""

import datetime

from airflow.sdk import DAG, task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Organiza el pipeline usando task groups
