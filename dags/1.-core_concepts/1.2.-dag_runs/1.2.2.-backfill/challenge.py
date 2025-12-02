"""
DESAFÍO: Sistema de Backfill Inteligente

Crea un sistema de backfill que detecta, valida y reprocesa datos
automáticamente cuando sea necesario.

CONTEXTO:
Eres el Data Engineer de una empresa de e-commerce. Tu data warehouse
tiene problemas ocasionales de calidad de datos y necesitas un sistema
robusto de backfill que:
- Detecte automáticamente datos faltantes o corruptos
- Valide calidad antes y después de procesamiento
- Permita reprocesamiento eficiente de períodos específicos
- Maneje dependencies entre tablas

REQUISITOS:

1. DAG: Data Quality Monitoring & Gap Detection
   - dag_id: 'backfill_challenge_quality_monitor'
   - schedule: '@hourly' (verifica calidad cada hora)
   - start_date: 3 días atrás
   - catchup: False
   - max_active_runs: 3
   - Tags: incluir 'challenge', 'dag_runs', 'backfill', 'monitoring'
   
   Pipeline (8 tareas mínimo):
   - start
   - scan_date_range (últimos 7 días)
   - detect_missing_partitions (identifica gaps)
   - validate_existing_data (checks de calidad)
   - check_row_counts (comparar con expected)
   - check_null_percentages
   - check_business_rules
   - log_quality_issues (guardar problemas encontrados)
   - end
   
   Validaciones en bash_command:
   - Simular detección de 3 fechas con datos faltantes
   - Simular detección de 2 fechas con calidad baja
   - Usar {{ ds }} para mostrar fecha de monitoreo

2. DAG: Backfill Execution Pipeline
   - dag_id: 'backfill_challenge_executor'
   - schedule: '@daily'
   - start_date: 30 días atrás
   - catchup: False (backfill será manual)
   - max_active_runs: 5 (procesar 5 días en paralelo)
   - Tags: incluir 'challenge', 'dag_runs', 'backfill', 'executor'
   
   Pipeline (10 tareas mínimo):
   - start
   - validate_partition_clean (verificar que partición no existe o está limpia)
   - create_staging_partition
   - extract_orders (orders para {{ ds }})
   - extract_customers (customers para {{ ds }})
   - validate_source_quality (checks en datos source)
   - join_and_transform
   - load_to_partition (específica de {{ ds }})
   - validate_output_quality
   - mark_partition_complete (metadata)
   - end
   
   Debe mostrar:
   - Uso de particiones por fecha
   - Idempotencia (limpiar antes de cargar)
   - Validaciones pre y post
   - Uso correcto de {{ ds }}, {{ ds_nodash }}

3. DAG: Cascade Backfill (Dependencies)
   - dag_id: 'backfill_challenge_cascade'
   - schedule: '@daily'
   - start_date: 7 días atrás
   - catchup: False
   - Tags: incluir 'challenge', 'dag_runs', 'backfill', 'cascade'
   
   Simula tabla dimension que depende de tabla fact.
   
   Pipeline (9 tareas mínimo):
   - start
   - check_fact_table_ready (verificar que fact data existe para {{ ds }})
   - wait_for_dependencies (simular espera)
   - extract_fact_data
   - compute_dimensions
   - validate_dimension_integrity (foreign keys, etc)
   - load_dimension_partition
   - update_dependencies_metadata
   - trigger_downstream_notification
   - end
   
   Debe explicar en doc_md:
   - Orden correcto de backfill cuando hay dependencies
   - Qué pasa si fact table no tiene datos para una fecha
   - Estrategia de backfill en cascada

RESTRICCIONES:
- Usar ÚNICAMENTE: BashOperator, EmptyOperator
- CADA bash_command debe mostrar uso de macros ({{ ds }}, {{ logical_date }}, etc)
- Incluir validaciones específicas con echo simulando checks reales
- Demostrar idempotencia en cada pipeline
- No usar sensors (solo bash checks)

DOCUMENTACIÓN REQUERIDA:
CADA DAG debe tener doc_md extenso explicando:
1. Propósito del DAG
2. Cómo se usa en el proceso de backfill
3. Comandos CLI para ejecutar backfill
4. Qué validaciones hace y por qué
5. Estrategia de recovery en caso de fallo

ESCENARIOS A CUBRIR:

**Monitoring DAG debe detectar:**
- Particiones faltantes (gaps en fechas)
- Datos con calidad baja (row count bajo, nulls altos)
- Violaciones de business rules

**Executor DAG debe manejar:**
- Backfill de fecha específica
- Backfill de rango de fechas
- Reprocessamiento idempotente
- Validación pre y post carga

**Cascade DAG debe manejar:**
- Dependencies entre tablas
- Validación de integridad referencial
- Backfill en orden correcto

COMANDOS CLI A INCLUIR EN DOC_MD:

```bash
# Backfill de rango
airflow dags backfill \\
    --start-date YYYY-MM-DD \\
    --end-date YYYY-MM-DD \\
    [dag_id]

# Backfill paralelo
airflow dags backfill \\
    --start-date YYYY-MM-DD \\
    --end-date YYYY-MM-DD \\
    --max-active-runs 5 \\
    [dag_id]

# Clear y re-ejecutar
airflow tasks clear \\
    --start-date YYYY-MM-DD \\
    --end-date YYYY-MM-DD \\
    [dag_id]
```

VALIDACIONES REQUERIDAS:
- Row count: "Expected 10000, Actual 10000"
- Null checks: "critical_field nulls: 0%"
- Business rules: "revenue = price * quantity (validated)"
- Referential integrity: "All foreign keys valid"
- Date range: "All dates within {{ ds }}"

TIPS:
- Usar particiones para eficiencia
- Validar antes de procesar (fail fast)
- Limpiar partición antes de cargar (idempotencia)
- Loggear todos los quality checks
- Pensar en el orden cuando hay dependencies
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa los 3 DAGs según los requisitos
# 1. Quality Monitor: Detecta problemas
# 2. Executor: Ejecuta backfill con validaciones
# 3. Cascade: Maneja dependencies entre tablas
