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

# Solución del challenge

def branch_skip_or_continue(**context):
    # Simulamos verificación de migración
    # En realidad debería consultar migration_log
    already_migrated = False  # Cambiar según consulta real
    if already_migrated:
        return 'skip_already_done'
    else:
        return 'extract_from_legacy'

with DAG(
    dag_id='migration_backfill_pipeline',
    start_date=datetime.datetime.now() - datetime.timedelta(days=90),
    schedule='@daily',
    catchup=True,
    max_active_runs=3,
    tags=['challenge', 'backfill', 'migration'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    identify_migration_date = BashOperator(
        task_id='identify_migration_date',
        bash_command='echo "Migrating date: {{ ds }}"',
    )
    
    check_if_already_migrated = BashOperator(
        task_id='check_if_already_migrated',
        bash_command='echo "Checking if {{ ds }} already migrated"',
    )
    
    branch_skip_or_continue_task = BranchPythonOperator(
        task_id='branch_skip_or_continue',
        python_callable=branch_skip_or_continue,
    )
    
    # Path 1: Si ya está migrado
    skip_already_done = EmptyOperator(task_id='skip_already_done')
    
    # Path 2: Si necesita migración
    extract_from_legacy = BashOperator(
        task_id='extract_from_legacy',
        bash_command='echo "Extracting from Oracle for {{ ds }}"',
    )
    
    validate_extract = BashOperator(
        task_id='validate_extract',
        bash_command='echo "Validating extract for {{ ds }}"',
    )
    
    # Validaciones paralelas
    validate_data_types = BashOperator(
        task_id='validate_data_types',
        bash_command='echo "Validating data types"',
    )
    
    validate_business_rules = BashOperator(
        task_id='validate_business_rules',
        bash_command='echo "Validating business rules"',
    )
    
    validate_referential_integrity = BashOperator(
        task_id='validate_referential_integrity',
        bash_command='echo "Validating referential integrity"',
    )
    
    transform_to_snowflake_format = BashOperator(
        task_id='transform_to_snowflake_format',
        bash_command='echo "Transforming to Snowflake format for {{ ds }}"',
    )
    
    stage_to_s3 = BashOperator(
        task_id='stage_to_s3',
        bash_command='echo "Uploading to s3://migration-bucket/{{ ds }}/"',
    )
    
    load_to_snowflake = BashOperator(
        task_id='load_to_snowflake',
        bash_command='echo "COPY INTO transactions_new FROM s3://migration-bucket/{{ ds }}/"',
    )
    
    verify_row_counts = BashOperator(
        task_id='verify_row_counts',
        bash_command='echo "Verifying row counts for {{ ds }}"',
    )
    
    update_migration_log = BashOperator(
        task_id='update_migration_log',
        bash_command='echo "Marking {{ ds }} as completed in migration_log"',
    )
    
    cleanup_temp_files = BashOperator(
        task_id='cleanup_temp_files',
        bash_command='echo "Cleaning temp files for {{ ds }}"',
        trigger_rule='all_done',
    )
    
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')
    
    # Dependencies
    start >> identify_migration_date >> check_if_already_migrated >> branch_skip_or_continue_task
    
    # Path 1: Skip
    branch_skip_or_continue_task >> skip_already_done >> end
    
    # Path 2: Migración completa
    branch_skip_or_continue_task >> extract_from_legacy >> validate_extract
    validate_extract >> [validate_data_types, validate_business_rules, validate_referential_integrity]
    [validate_data_types, validate_business_rules, validate_referential_integrity] >> transform_to_snowflake_format
    transform_to_snowflake_format >> stage_to_s3 >> load_to_snowflake >> verify_row_counts
    verify_row_counts >> update_migration_log >> cleanup_temp_files >> end
