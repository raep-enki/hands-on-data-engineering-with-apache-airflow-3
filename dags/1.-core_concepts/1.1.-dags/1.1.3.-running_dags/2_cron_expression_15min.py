"""
Expresión Cron - Cada 15 Minutos

Demuestra el uso de expresiones cron para control preciso del scheduling.
Formato cron: minuto hora dia_mes mes dia_semana
*/15 * * * * = Cada 15 minutos de cualquier hora, cualquier día
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id='cron_cada_15_minutos',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='*/15 * * * *',
    catchup=False,
    description='Ejecuta cada 15 minutos',
    tags=['example', 'core_concepts', 'dags', 'running_dags']
):
    tarea = BashOperator(
        task_id='monitoreo_frecuente',
        bash_command='echo "Monitoreo cada 15 minutos - $(date +%H:%M)"'
    )
