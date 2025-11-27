"""
Default Arguments - Timeouts

Demuestra la configuración de timeouts de ejecución para todas las tareas.
execution_timeout define el tiempo máximo que puede ejecutarse una tarea.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id='default_args_timeouts',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    default_args={
        'execution_timeout': timedelta(minutes=30),  # Máximo 30 minutos por tarea
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Configura timeout de 30 minutos para todas las tareas',
    tags=['example', 'core_concepts', 'dags', 'default_arguments']
):
    inicio = EmptyOperator(task_id='inicio')
    
    # Esta tarea fallará si tarda más de 30 minutos
    consultar_api = BashOperator(
        task_id='consultar_api_externa',
        bash_command='echo "Consultando API (timeout: 30 minutos)"'
    )
    
    # También con timeout de 30 minutos heredado
    procesar = BashOperator(
        task_id='procesar_respuesta',
        bash_command='echo "Procesando respuesta (máximo 30 minutos)"'
    )
    
    # Si falla, reintentará después de 5 minutos
    guardar = BashOperator(
        task_id='guardar_resultados',
        bash_command='echo "Guardando resultados (retry_delay: 5 min)"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> consultar_api >> procesar >> guardar >> fin
