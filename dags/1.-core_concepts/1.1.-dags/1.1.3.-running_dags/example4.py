"""
Expresión Cron - Días Laborales

Demuestra scheduling en días laborales (lunes a viernes) a una hora específica.
0 9 * * 1-5 = A las 9:00 AM de lunes (1) a viernes (5)
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id='cron_dias_laborales_9am',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='0 9 * * 1-5',
    catchup=False,
    description='Ejecuta de lunes a viernes a las 9 AM',
    tags=['example', 'core_concepts', 'dags', 'running_dags']
):
    inicio = EmptyOperator(task_id='inicio')
    
    reporte = BashOperator(
        task_id='reporte_inicio_dia',
        bash_command='echo "Reporte de inicio de jornada - $(date +%A) 9:00 AM"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> reporte >> fin
