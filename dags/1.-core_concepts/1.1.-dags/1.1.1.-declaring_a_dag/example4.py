"""
Pipeline de Respaldo y Limpieza - Declaración Mixta de DAG

Muestra uso tanto de context manager como DAG explícito en el mismo archivo.
Demuestra el uso de EmptyOperator para marcadores de flujo de trabajo.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Primer DAG - usando context manager
with DAG(
    dag_id='database_backup_pipeline',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='0 2 * * *',  # 2 AM diario
    catchup=False,
    description='Rutina diaria de respaldo de base de datos',
    tags=['example', 'core_concepts', 'dags', 'declaring_a_dag', 'backup']
):
    start_backup = EmptyOperator(task_id='start_backup')
    
    lock_database = BashOperator(
        task_id='lock_database',
        bash_command='echo "Adquiriendo bloqueo de base de datos"'
    )
    
    dump_data = BashOperator(
        task_id='dump_data',
        bash_command='echo "Creando volcado de base de datos"'
    )
    
    unlock_database = BashOperator(
        task_id='unlock_database',
        bash_command='echo "Liberando bloqueo de base de datos"'
    )
    
    compress_backup = BashOperator(
        task_id='compress_backup',
        bash_command='echo "Comprimiendo archivo de respaldo"'
    )
    
    upload_to_storage = BashOperator(
        task_id='upload_to_storage',
        bash_command='echo "Subiendo a almacenamiento en la nube"'
    )
    
    verify_upload = BashOperator(
        task_id='verify_upload',
        bash_command='echo "Verificando respaldo subido"'
    )
    
    cleanup_temp = BashOperator(
        task_id='cleanup_temp',
        bash_command='echo "Limpiando archivos temporales"'
    )
    
    backup_complete = EmptyOperator(task_id='backup_complete')
    
    # Dependencias
    start_backup >> lock_database >> dump_data >> unlock_database
    unlock_database >> compress_backup >> upload_to_storage >> verify_upload
    verify_upload >> cleanup_temp >> backup_complete


# Segundo DAG - usando instancia explícita
cleanup_dag = DAG(
    dag_id='old_data_cleanup_pipeline',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='0 3 * * 0',  # 3 AM cada domingo
    catchup=False,
    description='Limpieza semanal de datos antiguos y logs',
    tags=['example', 'core_concepts', 'dags', 'declaring_a_dag', 'cleanup']
)

with cleanup_dag:
    start_cleanup = EmptyOperator(task_id='start_cleanup')
    
    identify_old_data = BashOperator(
        task_id='identify_old_data',
        bash_command='echo "Identificando datos con más de 90 días"'
    )
    
    archive_old_data = BashOperator(
        task_id='archive_old_data',
        bash_command='echo "Archivando datos antiguos"'
    )
    
    delete_old_logs = BashOperator(
        task_id='delete_old_logs',
        bash_command='echo "Eliminando archivos de logs antiguos"'
    )
    
    vacuum_database = BashOperator(
        task_id='vacuum_database',
        bash_command='echo "Ejecutando vacuum/limpieza de base de datos"'
    )
    
    cleanup_complete = EmptyOperator(task_id='cleanup_complete')
    
    # Dependencias
    start_cleanup >> identify_old_data >> archive_old_data >> delete_old_logs
    delete_old_logs >> vacuum_database >> cleanup_complete
