"""
Pipeline de Procesamiento de Logs - Usando Pendulum para Fechas

Demuestra DAG con manejo de zonas horarias usando la librería pendulum.
Muestra patrón de ejecución de tareas en paralelo.
"""

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Usando pendulum para manejo apropiado de zonas horarias
with DAG(
    dag_id='log_processing_pipeline',
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    schedule='@daily',
    catchup=False,
    description='Procesar y analizar logs del sistema desde múltiples fuentes',
    tags=['example', 'declaring_a_dag']
):
    start = EmptyOperator(task_id='start')
    
    # Recolección paralela de logs desde diferentes fuentes
    collect_app_logs = BashOperator(
        task_id='collect_app_logs',
        bash_command='echo "Recolectando logs de aplicación"'
    )
    
    collect_web_logs = BashOperator(
        task_id='collect_web_logs',
        bash_command='echo "Recolectando logs del servidor web"'
    )
    
    collect_db_logs = BashOperator(
        task_id='collect_db_logs',
        bash_command='echo "Recolectando logs de base de datos"'
    )
    
    # Fusionar todos los logs
    merge_logs = BashOperator(
        task_id='merge_logs',
        bash_command='echo "Fusionando logs de todas las fuentes"'
    )
    
    # Procesar logs fusionados
    process_logs = BashOperator(
        task_id='process_logs',
        bash_command='echo "Procesando y parseando entradas de logs"'
    )
    
    # Archivar logs procesados
    archive = BashOperator(
        task_id='archive_logs',
        bash_command='echo "Archivando logs procesados"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Dependencias de tareas - patrón fan-out luego fan-in
    start >> [collect_app_logs, collect_web_logs, collect_db_logs]
    [collect_app_logs, collect_web_logs, collect_db_logs] >> merge_logs
    merge_logs >> process_logs >> archive >> end
