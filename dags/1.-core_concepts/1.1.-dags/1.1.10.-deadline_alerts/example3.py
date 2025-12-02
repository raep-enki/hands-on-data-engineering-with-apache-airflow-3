"""
Deadline Alerts - Pipeline Crítico de Tiempo Real

Demuestra un deadline muy corto para un pipeline crítico
que procesa datos en tiempo real.

El pipeline se ejecuta cada 5 minutos y debe completarse
en 3 minutos para mantener la latencia baja.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, AsyncCallback
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


async def alert_callback(**context):
    """Función async que se ejecuta cuando se alcanza el deadline."""
    print(f"🚨 DEADLINE CRÍTICO ALCANZADO para DAG: {context.get('dag_id')}")


with DAG(
    dag_id='deadline_alerts_realtime_critical',
    schedule='*/5 * * * *',  # cada 5 minutos
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'deadline_alerts'],
    # Deadline: 3 minutos (pipeline de tiempo real)
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=3),
        callback=AsyncCallback(callback_callable=alert_callback)
    )
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    ingest = BashOperator(
        task_id='ingest_stream',
        bash_command='echo "📥 Ingesta de stream en tiempo real"'
    )
    
    process = BashOperator(
        task_id='process_stream',
        bash_command='echo "⚡ Procesamiento rápido"'
    )
    
    publish = BashOperator(
        task_id='publish_results',
        bash_command='echo "📡 Publicando resultados"'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> ingest >> process >> publish >> end
