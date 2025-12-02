"""
Dynamic DAGs - Generación Básica de Tareas

Demuestra cómo generar tareas dinámicamente usando un bucle,
creando múltiples tareas similares sin repetir código.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='dynamic_basic_tasks',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dynamic_dags']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Lista de regiones para procesar
    regions = ['north', 'south', 'east', 'west']
    
    # Generar una tarea por cada región
    for region in regions:
        process_region = BashOperator(
            task_id=f'process_{region}_region',
            bash_command=f'echo "📍 Procesando región: {region}"'
        )
        
        start >> process_region
    
    end = EmptyOperator(task_id='end')
    
    # Conectar todas las tareas de regiones al final
    for region in regions:
        dag.get_task(f'process_{region}_region') >> end
