"""
Latest Only - Múltiples Ramas con Comportamiento Mixto

Demuestra cómo tener diferentes ramas en el DAG donde algunas
siempre se ejecutan y otras solo en el run más reciente.
"""

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

with DAG(
    dag_id='latest_only_mixed_branches',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'latest_only']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Rama 1: Procesamiento histórico (siempre se ejecuta)
    historical_etl = BashOperator(
        task_id='historical_etl',
        bash_command='echo "📊 ETL histórico - importante para backfill"'
    )
    
    store_historical = BashOperator(
        task_id='store_historical_data',
        bash_command='echo "💾 Almacenando datos históricos"'
    )
    
    # Checkpoint latest_only
    latest_only = LatestOnlyOperator(task_id='latest_only_check')
    
    # Rama 2: Operaciones en tiempo real (solo último run)
    publish_api = BashOperator(
        task_id='publish_to_api',
        bash_command='echo "🌐 Publicando a API en tiempo real"'
    )
    
    trigger_webhooks = BashOperator(
        task_id='trigger_webhooks',
        bash_command='echo "🔗 Disparando webhooks"'
    )
    
    update_frontend = BashOperator(
        task_id='update_frontend_data',
        bash_command='echo "💻 Actualizando datos del frontend"'
    )
    
    # Rama 3: Métricas y monitoreo (siempre se ejecuta)
    collect_metrics = BashOperator(
        task_id='collect_metrics',
        bash_command='echo "📈 Recolectando métricas - siempre"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # Join final
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    # Flujo:
    # Rama histórica: siempre se ejecuta
    start >> historical_etl >> store_historical
    
    # Rama en tiempo real: pasa por latest_only
    start >> latest_only >> [publish_api, trigger_webhooks, update_frontend]
    
    # Convergencia: métricas se ejecutan siempre
    [store_historical, publish_api, trigger_webhooks, update_frontend] >> collect_metrics >> end
