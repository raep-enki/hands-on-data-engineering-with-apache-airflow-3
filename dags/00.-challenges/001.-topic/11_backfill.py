"""
Challenge: Migración de Sistema Legacy a Uno Nuevo

La empresa decidió migrar de un sistema legacy (Oracle 11g) a uno moderno (Snowflake).
Tienes 90 días de datos históricos (enero - marzo 2024) que necesitas mover. No puedes hacerlo
todo de golpe: son 5 millones de registros por día que sobrecargarían la red y el sistema.

La estrategia: backfill controlado procesando día por día, pero corriendo máximo 3 días en paralelo
para optimizar sin saturar. Cada día es independiente: si falla uno, los demás continúan.

**El flujo de migración (idempotente y resiliente):**

`start` >> `identify_migration_date` (BashOperator - usa {{ logical_date }} para saber qué día migrar,
imprime fecha para debugging) >>

`check_if_already_migrated` (BashOperator - consulta tabla de control `migration_log` en Snowflake,
busca registro con fecha={{ logical_date }} y status='completed'. Si existe, retorna código 0 y
las tareas siguientes se skipean vía branch. Esto garantiza idempotencia) >>

`branch_skip_or_continue` (BranchPythonOperator - si ya migrado >> `skip_already_done`, 
si no migrado >> `extract_from_legacy`) >>

**Path 1: Si ya está migrado (idempotencia):**
`skip_already_done` (EmptyOperator - solo para logging) >> `end`

**Path 2: Si necesita migración (path principal):**
`extract_from_legacy` (BashOperator - ejecuta query en Oracle:
`SELECT * FROM transactions WHERE DATE(created_at) = '{{ ds }}'`
Export a CSV en /tmp/migration_{{ ds }}.csv, puede tardar 10 min por día) >>

`validate_extract` (BashOperator - verifica: archivo existe, no está vacío, count de filas coincide
con lo esperado para esa fecha, no hay filas con NULL en campos críticos) >>

Se ramifica en 3 validaciones paralelas:
- `validate_data_types` (BashOperator - verifica que columnas tengan tipos correctos)
- `validate_business_rules` (BashOperator - amounts > 0, dates válidas, IDs únicos)
- `validate_referential_integrity` (BashOperator - customer_ids existen en tabla customers)

`[todas las validaciones]` >> `transform_to_snowflake_format` (BashOperator - convierte:
fechas Oracle → timestamps Snowflake, NULLs Oracle → NULL Snowflake, encoding ISO-8859-1 → UTF-8) >>

`stage_to_s3` (BashOperator - sube CSV transformado a S3: s3://migration-bucket/{{ ds }}/data.csv
necesario porque Snowflake carga desde S3 más rápido que directo) >>

`load_to_snowflake` (BashOperator - ejecuta COPY INTO en Snowflake:
```sql
COPY INTO transactions_new 
FROM s3://migration-bucket/{{ ds }}/
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1)
ON_ERROR = 'ABORT'
```
Si hay error, falla el día completo pero otros días continúan) >>

`verify_row_counts` (BashOperator - compara count en Oracle vs Snowflake para {{ ds }},
deben ser exactamente iguales. Si difiere, falla y rollback) >>

`update_migration_log` (BashOperator - inserta en tabla de control:
`INSERT INTO migration_log (date, status, row_count, completed_at) 
VALUES ('{{ ds }}', 'completed', {{ row_count }}, NOW())`
Esto previene reprocesar en futuras corridas) >>

`cleanup_temp_files` (BashOperator con `trigger_rule='all_done'` - borra CSV de /tmp/
y archivos de S3 staging, se ejecuta SIEMPRE incluso si hubo falla) >>

`end` (EmptyOperator con `trigger_rule='none_failed_min_one_success'`)

**Resiliencia ante fallas:**
Si el día 45 falla (datos corruptos, conexión perdida), los días 46-90 continúan independientes.
Puedes re-correr solo el día 45 después de arreglar el problema.

**Control de paralelismo:**
`max_active_runs=3` significa que procesa máximo 3 días simultáneos. Airflow automáticamente
encola los demás y los procesa conforme se van liberando slots. Esto evita saturar:
- Network bandwidth (3 CSVs viajando simultáneos, no 90)
- Oracle connections (3 queries concurrentes, no 90)
- Snowflake warehouse (3 cargas paralelas, no 90)

**Tracking de progreso:**
La tabla `migration_log` te permite saber en todo momento:
- Qué días completaron: `SELECT * FROM migration_log WHERE status='completed'`
- Qué días faltan: comparar rango de fechas vs registros en log
- Qué días fallaron: buscar en Airflow logs donde status=failed

**Idempotencia garantizada:**
Si corres el DAG dos veces, `check_if_already_migrated` detecta que ya existe en `migration_log`
y skippea todo el procesamiento. No hay duplicados.

**Configuración técnica:**
- DAG ID: `migration_backfill_pipeline`
- Schedule: @daily
- Start date: 90 días atrás desde hoy (usa `datetime.datetime.now() - datetime.timedelta(days=90)`)
- **Catchup: True** (crítico! debe procesar los 90 días)
- **max_active_runs: 3** (controla paralelismo, no saturar sistemas)
- Tags: `['challenge', 'backfill', 'migration']`
- Total: 15 tareas con paths condicionales y limpieza garantizada
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Diseña el pipeline de migración con backfill controlado
