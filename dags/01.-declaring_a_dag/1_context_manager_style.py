"""
Pipeline ETL Simple - Estilo Context Manager

Demuestra el estilo preferido de context manager para declaración de DAGs.
Usa solo operadores básicos y dependencias de tareas - sin paso de datos aún.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Estilo context manager - preferido para la mayoría de DAGs
with DAG(
    dag_id='simple_etl_pipeline',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'declaring_a_dag']
):
    # Tarea de inicio - marca el comienzo del pipeline
    start = EmptyOperator(task_id='start')
    
    # Extraer - simular extracción de datos
    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "Extrayendo datos del sistema origen"'
    )
    
    # Transformar - simular transformación de datos
    transform = BashOperator(
        task_id='transform_data',
        bash_command='echo "Transformando datos: limpieza, filtrado, enriquecimiento"'
    )
    
    # Cargar - simular carga de datos
    load = BashOperator(
        task_id='load_data',
        bash_command='echo "Cargando datos al almacén destino"'
    )
    
    # Tarea final - marca la finalización
    end = EmptyOperator(task_id='end')
    
    # Definir dependencias de tareas - pipeline lineal
    start >> extract >> transform >> load >> end
