"""
Deadline Alerts - Referencia LOGICAL_DATE

Demuestra el uso de DAGRUN_LOGICAL_DATE como referencia de tiempo.
Esta referencia se basa en la fecha lógica del DAG run
(logical_date), que representa el inicio del período de datos.

Útil para deadlines basados en el intervalo de datos
que se está procesando.
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
    dag_id='deadline_alerts_start_reference',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'deadline_alerts'],
    # Si no se completa en 45 minutos desde el inicio del intervalo lógico
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(minutes=45),
        callback=AsyncCallback(callback_callable=alert_callback)
    )
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract = BashOperator(
        task_id='extract',
        bash_command='echo "📥 Extrayendo datos"; sleep 1'
    )
    
    process = BashOperator(
        task_id='process',
        bash_command='echo "⚙️ Procesando con deadline desde logical_date"; sleep 1'
    )
    
    load = BashOperator(
        task_id='load',
        bash_command='echo "📤 Cargando resultados"; sleep 1'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> extract >> process >> load >> end
