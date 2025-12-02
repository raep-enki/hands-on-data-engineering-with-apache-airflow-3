"""
Deadline Alerts - Concepto Básico

Demuestra cómo configurar deadline alerts para un DAG.
Un deadline alert se dispara cuando el DAG no se completa dentro
del tiempo esperado desde un punto de referencia.

En este ejemplo, si el DAG no termina en 30 minutos desde que
fue encolado, se generará una alerta.
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


with DAG(
    dag_id='deadline_alerts_basic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'deadline_alerts'],
    # Deadline: 30 minutos desde que el DAG run fue encolado
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=30),
        callback=AsyncCallback(callback_callable=alert_callback)
    )
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "📥 Extrayendo datos"; sleep 2'
    )
    
    transform = BashOperator(
        task_id='transform_data',
        bash_command='echo "⚙️ Transformando datos"; sleep 2'
    )
    
    load = BashOperator(
        task_id='load_data',
        bash_command='echo "📤 Cargando datos"; sleep 2'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> extract >> transform >> load >> end
