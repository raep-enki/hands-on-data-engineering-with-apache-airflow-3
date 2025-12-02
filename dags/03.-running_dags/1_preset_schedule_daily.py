"""
Schedule Preset @daily - Ejecución Diaria

Demuestra el uso del preset @daily para ejecutar un DAG una vez al día (medianoche).
Este es uno de los schedules más comunes en data engineering.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id='preset_daily',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    description='Se ejecuta diariamente a medianoche',
    tags=['example', 'running_dags']
):
    inicio = EmptyOperator(task_id='inicio')
    
    backup = BashOperator(
        task_id='backup_diario',
        bash_command='echo "Ejecutando backup diario - Fecha: $(date +%Y-%m-%d)"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> backup >> fin
