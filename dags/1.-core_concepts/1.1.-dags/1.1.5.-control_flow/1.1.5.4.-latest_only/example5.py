"""
Latest Only - Múltiples Checkpoints

Demuestra el uso de múltiples LatestOnlyOperators en diferentes
partes del DAG para controlar granularmente qué se ejecuta solo
en el run más reciente.
"""

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

with DAG(
    dag_id='latest_only_multiple_checkpoints',
    schedule='@hourly',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'latest_only']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Procesamiento base (siempre)
    ingest_data = BashOperator(
        task_id='ingest_raw_data',
        bash_command='echo "📥 Ingesta de datos raw"'
    )
    
    # Primera rama con checkpoint
    latest_check_1 = LatestOnlyOperator(task_id='latest_check_notifications')
    
    # Notificaciones inmediatas (solo último run)
    send_sms = BashOperator(
        task_id='send_sms_alerts',
        bash_command='echo "📱 Enviando SMS - SOLO último run"'
    )
    
    send_push = BashOperator(
        task_id='send_push_notifications',
        bash_command='echo "🔔 Enviando push notifications - SOLO último run"'
    )
    
    # Procesamiento intermedio (siempre)
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='echo "⚙️ Transformando datos"'
    )
    
    # Segunda rama con checkpoint diferente
    latest_check_2 = LatestOnlyOperator(task_id='latest_check_external_systems')
    
    # Integraciones externas (solo último run)
    sync_crm = BashOperator(
        task_id='sync_to_crm',
        bash_command='echo "🔄 Sincronizando con CRM - SOLO último run"'
    )
    
    update_erp = BashOperator(
        task_id='update_erp_system',
        bash_command='echo "🏢 Actualizando ERP - SOLO último run"'
    )
    
    # Almacenamiento final (siempre)
    store_data = BashOperator(
        task_id='store_processed_data',
        bash_command='echo "💾 Almacenando datos procesados"'
    )
    
    # Métricas finales (siempre)
    metrics = BashOperator(
        task_id='collect_metrics',
        bash_command='echo "📊 Recolectando métricas"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # Flujo con dos checkpoints independientes:
    
    # Rama 1: Notificaciones inmediatas
    start >> ingest_data >> latest_check_1 >> [send_sms, send_push]
    
    # Procesamiento central
    ingest_data >> transform_data >> store_data
    
    # Rama 2: Integraciones externas
    transform_data >> latest_check_2 >> [sync_crm, update_erp]
    
    # Convergencia final
    [send_sms, send_push, store_data, sync_crm, update_erp] >> metrics >> end
