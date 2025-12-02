"""
Default Arguments Completos - Configuración de Producción

Demuestra una configuración completa de default_args para un ambiente de producción.
Incluye reintentos, timeouts, dependencias y notificaciones.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id='default_args_production',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='0 2 * * *',  # 2 AM diario
    catchup=False,
    default_args={
        # Propietario y notificaciones
        'owner': 'equipo_produccion',
        'email': ['produccion@empresa.com', 'ops@empresa.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        
        # Reintentos
        'retries': 3,
        'retry_delay': timedelta(minutes=10),
        'retry_exponential_backoff': True,
        
        # Timeouts
        'execution_timeout': timedelta(hours=2),
        
        # Dependencias (tareas esperan que upstream esté exitoso)
        'depends_on_past': False,
        'wait_for_downstream': False
    },
    description='Configuración completa de producción con default_args',
    tags=['example', 'default_arguments']
):
    inicio = EmptyOperator(task_id='inicio')
    
    # Pipeline de ETL con configuración de producción
    extraer = BashOperator(
        task_id='extraer_datos_produccion',
        bash_command='echo "Extrayendo datos (retries=3, exponential backoff)"'
    )
    
    validar = BashOperator(
        task_id='validar_calidad',
        bash_command='echo "Validando calidad de datos"'
    )
    
    transformar = BashOperator(
        task_id='transformar_datos',
        bash_command='echo "Aplicando transformaciones de negocio"'
    )
    
    cargar = BashOperator(
        task_id='cargar_warehouse',
        bash_command='echo "Cargando a data warehouse"'
    )
    
    notificar = BashOperator(
        task_id='notificar_completado',
        bash_command='echo "Notificando completación exitosa"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> extraer >> validar >> transformar >> cargar >> notificar >> fin
