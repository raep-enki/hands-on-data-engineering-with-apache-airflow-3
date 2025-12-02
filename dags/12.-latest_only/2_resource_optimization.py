"""
Latest Only - Optimización de Recursos en Backfill

Demuestra un caso real: durante backfill queremos procesar datos
históricos, pero NO queremos consumir recursos caros (APIs externas,
notificaciones, etc.) excepto en el run más reciente.
"""

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

with DAG(
    dag_id='latest_only_resource_optimization',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'latest_only']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Fase 1: Extracción y procesamiento (siempre, es el core del backfill)
    extract_sales = BashOperator(
        task_id='extract_sales_data',
        bash_command='echo "📥 Extrayendo datos de ventas"'
    )
    
    process_sales = BashOperator(
        task_id='process_sales',
        bash_command='echo "⚙️ Procesando y agregando ventas"'
    )
    
    store_warehouse = BashOperator(
        task_id='store_in_warehouse',
        bash_command='echo "💾 Guardando en data warehouse"'
    )
    
    # Checkpoint: operaciones caras solo en último run
    latest_only = LatestOnlyOperator(task_id='check_if_latest')
    
    # Fase 2: Operaciones caras (solo último run)
    # Estas consumen APIs, créditos, o recursos limitados
    
    call_external_api = BashOperator(
        task_id='enrich_with_external_api',
        bash_command='echo "🌐 Llamando API externa ($$$ costoso)"'
    )
    
    send_slack_notification = BashOperator(
        task_id='notify_team_slack',
        bash_command='echo "💬 Enviando notificación a Slack"'
    )
    
    send_email_report = BashOperator(
        task_id='send_email_report',
        bash_command='echo "📧 Enviando reporte por email"'
    )
    
    trigger_ml_training = BashOperator(
        task_id='trigger_ml_model_training',
        bash_command='echo "🤖 Disparando entrenamiento de ML (GPU costoso)"'
    )
    
    # Fase 3: Validación y cierre (siempre)
    validate = BashOperator(
        task_id='validate_data_quality',
        bash_command='echo "✅ Validando calidad de datos"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    log_metrics = BashOperator(
        task_id='log_execution_metrics',
        bash_command='echo "📊 Registrando métricas de ejecución"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # Flujo optimizado para backfill:
    # 1. Procesamiento core: siempre (necesario para datos históricos)
    start >> extract_sales >> process_sales >> store_warehouse
    
    # 2. Operaciones caras: solo último run
    store_warehouse >> latest_only >> [
        call_external_api,
        send_slack_notification, 
        send_email_report,
        trigger_ml_training
    ]
    
    # 3. Validación: siempre (para verificar todos los runs)
    [store_warehouse, call_external_api, send_slack_notification, 
     send_email_report, trigger_ml_training] >> validate >> log_metrics >> end
