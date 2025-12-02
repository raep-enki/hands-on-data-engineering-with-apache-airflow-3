"""
DESAFÍO: Sistema Multi-Tier con Deadlines Apropiados

Crea un sistema de 4 DAGs con diferentes niveles de criticidad (tiers)
y configura deadlines apropiados para cada uno según su SLA.

CONTEXTO:
Tienes un data platform con diferentes tipos de pipelines, cada uno
con diferentes SLAs y niveles de criticidad:

TIER 1 (Critical): Impacto directo en operaciones de negocio
TIER 2 (High): Impacto en reportes ejecutivos
TIER 3 (Medium): Impacto en análisis y optimización
TIER 4 (Low): Impacto en datos históricos y ML

REQUISITOS:

1. DAG TIER 1 - Payment Processing Pipeline:
   - dag_id: 'deadline_challenge_tier1_payments'
   - schedule: '*/15 * * * *' (cada 15 minutos)
   - Deadline: 12 minutos desde queued
   - Reference: DAGRUN_QUEUED_AT
   - Callback: AsyncCallback(callback_callable=alert_callback)
   - Tags: incluir 'tier1', 'critical', 'payments'
   
   Pipeline (7 tareas mínimo):
   - start
   - extract_transactions
   - validate_fraud_checks
   - verify_account_balance
   - process_payment
   - update_ledger
   - notify_confirmation
   - end

2. DAG TIER 2 - Executive Dashboard Refresh:
   - dag_id: 'deadline_challenge_tier2_dashboard'
   - schedule: '0 6,12,18 * * *' (6 AM, 12 PM, 6 PM)
   - Deadline: 90 minutos desde logical date
   - Reference: DAGRUN_LOGICAL_DATE
   - Callback: AsyncCallback(callback_callable=alert_callback)
   - Tags: incluir 'tier2', 'high', 'dashboard'
   
   Pipeline (8 tareas mínimo):
   - start
   - extract_kpi_sources (3 fuentes en paralelo)
   - calculate_metrics
   - generate_charts
   - publish_dashboard
   - send_summary_email
   - end

3. DAG TIER 3 - Customer Analytics Pipeline:
   - dag_id: 'deadline_challenge_tier3_analytics'
   - schedule: '@daily' (1 vez al día)
   - Deadline: 4 horas desde logical date
   - Reference: DAGRUN_LOGICAL_DATE
   - Callback: AsyncCallback(callback_callable=alert_callback)
   - Tags: incluir 'tier3', 'medium', 'analytics'
   
   Pipeline (9 tareas mínimo):
   - start
   - extract_customer_data
   - extract_behavior_data
   - merge_datasets
   - calculate_segments
   - calculate_ltv
   - generate_insights
   - update_analytics_tables
   - trigger_ml_refresh
   - end

4. DAG TIER 4 - Historical Data Archive:
   - dag_id: 'deadline_challenge_tier4_archive'
   - schedule: '0 1 * * 0' (Domingos 1 AM)
   - Deadline: 18 horas desde logical date
   - Reference: DAGRUN_LOGICAL_DATE
   - Callback: AsyncCallback(callback_callable=alert_callback)
   - Tags: incluir 'tier4', 'low', 'archive'
   
   Pipeline (8 tareas mínimo):
   - start
   - identify_old_data
   - validate_completeness
   - compress_data
   - encrypt_sensitive_data
   - upload_to_s3_glacier
   - update_catalog
   - cleanup_local
   - end

RESTRICCIONES:
- Usar ÚNICAMENTE: BashOperator, EmptyOperator
- CADA DAG debe tener su deadline configurado correctamente
- Los deadlines deben reflejar la criticidad del tier
- Incluir todas las tags requeridas
- Los bash_command deben ser descriptivos
- Mínimo el número de tareas especificado para cada DAG

DOCUMENTACIÓN REQUERIDA:
Cada DAG debe tener un docstring explicando:
- Propósito del pipeline
- Por qué ese deadline es apropiado
- Qué pasa si se pierde el deadline
- Tier y criticidad

TIPS:
- Tier 1 (Critical): Deadlines muy cortos (minutos)
- Tier 2 (High): Deadlines moderados (1-2 horas)
- Tier 3 (Medium): Deadlines flexibles (2-6 horas)
- Tier 4 (Low): Deadlines amplios (6+ horas)
- La referencia de tiempo debe ser apropiada para cada caso
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, AsyncCallback
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


async def alert_callback(**context):
    """Función async que se ejecuta cuando se alcanza el deadline."""
    print(f"⚠️ DEADLINE ALCANZADO para DAG: {context.get('dag_id')}")


# TODO: Implementa los 4 DAGs según los requisitos
# Nota: Usa DeadlineReference.DAGRUN_QUEUED_AT o DeadlineReference.DAGRUN_LOGICAL_DATE
# Recuerda que DeadlineAlert requiere el parámetro callback=AsyncCallback(callback_callable=alert_callback)
