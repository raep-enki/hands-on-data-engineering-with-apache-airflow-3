"""
Default Arguments Básicos - Reintentos

Demuestra el uso de default_args para configurar reintentos en todas las tareas del DAG.
Los default_args evitan repetir configuración en cada tarea.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id='default_args_retries',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'retries': 2  # Todas las tareas se reintentarán 2 veces en caso de fallo
    },
    description='Demuestra configuración de reintentos con default_args',
    tags=['example', 'core_concepts', 'dags', 'default_arguments']
):
    inicio = EmptyOperator(task_id='inicio')
    
    # Esta tarea heredará retries=2 de default_args
    extraer = BashOperator(
        task_id='extraer_datos',
        bash_command='echo "Extrayendo datos (se reintentará 2 veces si falla)"'
    )
    
    # Esta tarea también heredará retries=2
    procesar = BashOperator(
        task_id='procesar_datos',
        bash_command='echo "Procesando datos (retries=2 heredado)"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> extraer >> procesar >> fin
