"""
DAG Dentro de Función - Patrón Factory

Demuestra cómo crear un DAG dentro de una función factory y asignarlo a módulo.
La función devuelve el DAG y debe ser asignado a una variable de módulo para ser descubierto.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def create_processing_dag():
    """
    Función factory que crea y retorna un DAG.
    """
    dag = DAG(
        dag_id='visible_function_dag',
        start_date=datetime.datetime(2024, 1, 1),
        schedule='@daily',
        catchup=False,
        description='DAG creado en función pero asignado a módulo',
        tags=['example', 'core_concepts', 'dags', 'loading_dags']
    )
    
    with dag:
        start = EmptyOperator(task_id='start')
        
        process = BashOperator(
            task_id='process',
            bash_command='echo "Este DAG SÍ es visible porque fue asignado a variable de módulo"'
        )
        
        end = EmptyOperator(task_id='end')
        
        start >> process >> end
    
    return dag


# IMPORTANTE: Asignar a una variable de módulo para que sea descubierto
visible_dag = create_processing_dag()
