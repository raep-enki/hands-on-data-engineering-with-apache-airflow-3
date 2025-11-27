"""
Default Arguments - Propietario y Email

Demuestra la configuración de propietario y notificaciones por email.
Estos valores se heredan por todas las tareas del DAG.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id='default_args_owner_email',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'equipo_data_engineering',
        'email': ['data-team@empresa.com'],
        'email_on_failure': True,
        'email_on_retry': False
    },
    description='Configura propietario y notificaciones para todas las tareas',
    tags=['example', 'core_concepts', 'dags', 'default_arguments']
):
    inicio = EmptyOperator(task_id='inicio')
    
    # Todas estas tareas pertenecen a 'equipo_data_engineering'
    # y enviarán email en caso de fallo
    backup = BashOperator(
        task_id='backup_datos',
        bash_command='echo "Ejecutando backup (owner: equipo_data_engineering)"'
    )
    
    validar = BashOperator(
        task_id='validar_integridad',
        bash_command='echo "Validando integridad (notificará por email si falla)"'
    )
    
    archivar = BashOperator(
        task_id='archivar_backup',
        bash_command='echo "Archivando backup"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> backup >> validar >> archivar >> fin
