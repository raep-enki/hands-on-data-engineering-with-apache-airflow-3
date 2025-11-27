"""
DAG con Context Manager - Descubrimiento Automático

Demuestra el uso del context manager que crea y asigna el DAG automáticamente.
El context manager es el estilo preferido en Airflow 3.x.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Context manager también es descubierto automáticamente
# El context manager crea y asigna el DAG a una variable implícitamente
with DAG(
    dag_id='discovered_context_manager',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    description='DAG con context manager - será descubierto',
    tags=['example', 'core_concepts', 'dags', 'loading_dags']
):
    
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Tarea 1 ejecutándose"'
    )
    
    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "Tarea 2 ejecutándose"'
    )
    
    task3 = EmptyOperator(task_id='task3')
    
    task1 >> task2 >> task3
