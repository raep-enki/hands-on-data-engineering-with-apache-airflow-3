"""
Branching Básico con BranchPythonOperator

Demuestra cómo usar branching para elegir dinámicamente qué ruta tomar en el DAG.
Este ejemplo usa una decisión aleatoria para seleccionar entre tres niveles de prioridad.
"""

import datetime
import random

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

dag = DAG(
    dag_id='branching_basic',
    start_date=datetime.datetime(2021, 1, 1),
    schedule='@daily',
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'branching']
)


def choose_branch(**context):
    """
    Función que decide qué rama seguir.
    Debe retornar el task_id de la tarea a ejecutar.
    """
    # Simulamos una decisión aleatoria
    value = random.randint(1, 10)
    
    if value >= 7:
        return 'high_priority_task'
    elif value >= 4:
        return 'medium_priority_task'
    else:
        return 'low_priority_task'


start_op = EmptyOperator(task_id='start', dag=dag)

branch_op = BranchPythonOperator(
    task_id='branch_decision',
    python_callable=choose_branch,
    dag=dag,
)

high_priority = EmptyOperator(task_id='high_priority_task', dag=dag)
medium_priority = EmptyOperator(task_id='medium_priority_task', dag=dag)
low_priority = EmptyOperator(task_id='low_priority_task', dag=dag)

end_op = EmptyOperator(task_id='end', dag=dag)

start_op >> branch_op >> [high_priority, medium_priority, low_priority] >> end_op
