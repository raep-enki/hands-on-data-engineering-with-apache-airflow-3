"""
Override de Default Arguments - Configuración Específica por Tarea

Demuestra cómo una tarea puede sobrescribir valores de default_args.
Las configuraciones específicas de tarea tienen prioridad sobre default_args.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id='default_args_override',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=10)
    },
    description='Demuestra cómo sobrescribir default_args en tareas específicas',
    tags=['example', 'default_arguments']
):
    inicio = EmptyOperator(task_id='inicio')
    
    # Esta tarea usa los valores de default_args (retries=2, timeout=10 min)
    tarea_normal = BashOperator(
        task_id='tarea_con_defaults',
        bash_command='echo "Usando defaults: retries=2, timeout=10 min"'
    )
    
    # Esta tarea SOBRESCRIBE retries a 5 (en lugar de 2)
    tarea_critica = BashOperator(
        task_id='tarea_critica',
        bash_command='echo "Tarea crítica: retries=5 (override), timeout=10 min (heredado)"',
        retries=5  # Sobrescribe el valor de default_args
    )
    
    # Esta tarea sobrescribe el timeout a 30 minutos
    tarea_lenta = BashOperator(
        task_id='tarea_lenta',
        bash_command='echo "Proceso lento: timeout=30 min (override), retries=2 (heredado)"',
        execution_timeout=timedelta(minutes=30)  # Sobrescribe el timeout
    )
    
    # Esta tarea sobrescribe múltiples valores
    tarea_especial = BashOperator(
        task_id='tarea_especial',
        bash_command='echo "Configuración personalizada completa"',
        retries=0,  # Sin reintentos
        execution_timeout=timedelta(hours=1)  # 1 hora de timeout
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> [tarea_normal, tarea_critica, tarea_lenta, tarea_especial] >> fin
