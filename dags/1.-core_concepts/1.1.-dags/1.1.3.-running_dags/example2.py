"""
Schedule Preset @hourly - Ejecución Horaria

Demuestra el uso del preset @hourly para ejecutar un DAG cada hora (inicio de hora).
Útil para monitoreo frecuente y procesamiento en casi-tiempo-real.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id='preset_hourly',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    description='Se ejecuta cada hora',
    tags=['example', 'core_concepts', 'dags', 'running_dags']
):
    tarea = BashOperator(
        task_id='reporte_horario',
        bash_command='echo "Generando reporte horario - Hora: $(date +%H:00)"'
    )
