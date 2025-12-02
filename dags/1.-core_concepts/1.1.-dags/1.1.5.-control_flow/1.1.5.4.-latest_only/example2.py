"""
Latest Only - Con Trigger Rules

Demuestra cómo combinar LatestOnlyOperator con trigger rules
para crear flujos más sofisticados.

Algunas tareas necesitan ejecutarse siempre, incluso si están
después del LatestOnlyOperator. Para esto usamos ALL_DONE.
"""

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

with DAG(
    dag_id='latest_only_with_trigger_rules',
    schedule='@hourly',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'latest_only']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Tarea que siempre se ejecuta
    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "📥 Extrayendo datos - siempre"'
    )
    
    # Checkpoint de latest_only
    latest_only = LatestOnlyOperator(task_id='latest_only_checkpoint')
    
    # Tareas que solo se ejecutan en el run más reciente
    send_notification = BashOperator(
        task_id='send_real_time_notification',
        bash_command='echo "🔔 Notificación en tiempo real - SOLO último run"'
    )
    
    update_cache = BashOperator(
        task_id='update_cache',
        bash_command='echo "💾 Actualizando cache - SOLO último run"'
    )
    
    # Tarea que SIEMPRE se ejecuta, incluso después de latest_only
    # Usa ALL_DONE para ejecutarse sin importar si latest_only la skip
    audit = BashOperator(
        task_id='audit_execution',
        bash_command='echo "📋 Auditoría - SIEMPRE se ejecuta (ALL_DONE)"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # Tarea final que también se ejecuta siempre
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='echo "🧹 Limpieza - SIEMPRE se ejecuta (ALL_DONE)"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # Flujo:
    # start -> extract (siempre)
    # extract -> latest_only (checkpoint)
    # latest_only -> notification/cache (solo último run)
    # notification/cache -> audit (siempre, con ALL_DONE)
    # audit -> cleanup (siempre, con ALL_DONE)
    
    start >> extract >> latest_only >> [send_notification, update_cache] >> audit >> cleanup
