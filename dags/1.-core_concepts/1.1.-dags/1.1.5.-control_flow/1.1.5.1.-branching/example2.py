"""
Branching Basado en Día de la Semana

Demuestra cómo crear un DAG que ejecuta diferentes tareas según el día de la semana.
Útil para procesos que tienen lógica diferente dependiendo del día.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def decide_by_weekday(**context):
    """
    Decide qué proceso ejecutar según el día de la semana.
    Retorna el task_id de la tarea a ejecutar.
    """
    logical_date = context['logical_date']
    weekday = logical_date.weekday()  # 0=Monday, 6=Sunday
    
    if weekday == 0:  # Lunes
        return 'proceso_inicio_semana'
    elif weekday == 4:  # Viernes
        return 'proceso_fin_semana'
    elif weekday in [5, 6]:  # Fin de semana
        return 'proceso_mantenimiento'
    else:  # Martes, Miércoles, Jueves
        return 'proceso_rutinario'


with DAG(
    dag_id='branching_by_weekday',
    start_date=datetime.datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'branching']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    branch = BranchPythonOperator(
        task_id='decidir_por_dia',
        python_callable=decide_by_weekday,
    )
    
    # Diferentes procesos según el día
    inicio_semana = BashOperator(
        task_id='proceso_inicio_semana',
        bash_command='echo "Ejecutando proceso especial de inicio de semana"'
    )
    
    fin_semana = BashOperator(
        task_id='proceso_fin_semana',
        bash_command='echo "Ejecutando proceso especial de fin de semana"'
    )
    
    mantenimiento = BashOperator(
        task_id='proceso_mantenimiento',
        bash_command='echo "Ejecutando tareas de mantenimiento de fin de semana"'
    )
    
    rutinario = BashOperator(
        task_id='proceso_rutinario',
        bash_command='echo "Ejecutando proceso rutinario diario"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    # Estructura de dependencias
    inicio >> branch >> [inicio_semana, fin_semana, mantenimiento, rutinario] >> fin
