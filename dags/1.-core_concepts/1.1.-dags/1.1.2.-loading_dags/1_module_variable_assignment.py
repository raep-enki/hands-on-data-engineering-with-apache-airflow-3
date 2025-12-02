"""
DAG Asignado a Variable de Módulo - Patrón de Descubrimiento

Demuestra cómo Airflow descubre DAGs buscando objetos DAG en variables del módulo.
Este es el patrón básico: asignar el DAG a una variable a nivel de módulo.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# DAG asignado a variable de módulo - SERÁ DESCUBIERTO
# Este es el patrón más simple y directo
dag_discovered = DAG(
    dag_id='discovered_module_variable',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    description='DAG asignado a variable de módulo - será descubierto',
    tags=['example', 'core_concepts', 'dags', 'loading_dags']
)

# Crear tareas para este DAG
start = EmptyOperator(
    task_id='start',
    dag=dag_discovered
)

process = BashOperator(
    task_id='process_data',
    bash_command='echo "Procesando datos en DAG descubierto"',
    dag=dag_discovered
)

end = EmptyOperator(
    task_id='end',
    dag=dag_discovered
)

# Definir dependencias
start >> process >> end
