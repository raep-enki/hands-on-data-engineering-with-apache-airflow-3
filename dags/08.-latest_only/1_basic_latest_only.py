"""
Latest Only - Concepto Básico

Demuestra el uso de LatestOnlyOperator para evitar ejecutar tareas
en DAG runs antiguos durante backfill o catchup.

El LatestOnlyOperator permite que solo el DAG run más reciente ejecute
las tareas downstream, mientras que DAG runs antiguos las omiten.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

with DAG(
    dag_id='latest_only_basic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'latest_only']
) as dag:
    
    # Tareas que siempre se ejecutan
    start = EmptyOperator(task_id='start')
    
    historical_data = BashOperator(
        task_id='process_historical_data',
        bash_command='echo "📊 Procesando datos históricos - se ejecuta en TODOS los runs"'
    )
    
    # LatestOnlyOperator: Punto de control
    latest_only = LatestOnlyOperator(task_id='latest_only')
    
    # Tareas downstream del LatestOnlyOperator
    # Solo se ejecutan en el DAG run más reciente
    send_email = BashOperator(
        task_id='send_daily_email',
        bash_command='echo "📧 Enviando email - SOLO en el run más reciente"'
    )
    
    update_dashboard = BashOperator(
        task_id='update_live_dashboard',
        bash_command='echo "📈 Actualizando dashboard - SOLO en el run más reciente"'
    )
    
    # Flujo:
    # - historical_data se ejecuta siempre (para catchup/backfill)
    # - latest_only decide si continuar o no
    # - send_email y update_dashboard solo en el último run
    start >> historical_data >> latest_only >> [send_email, update_dashboard]
