"""
Branching con Múltiples Niveles de Decisión

Demuestra cómo usar branching anidado o secuencial para decisiones más complejas
basadas en múltiples criterios.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def first_decision(**context):
    """Primera decisión: tipo de entorno basado en día de semana"""
    weekday = context['logical_date'].weekday()
    
    if weekday in [5, 6]:  # Fin de semana
        return 'analizar_fin_semana'
    else:  # Días laborales
        return 'analizar_dia_laboral'


def decide_weekend_processing(**context):
    """Segunda decisión: procesamiento de fin de semana según hora"""
    hour = context['logical_date'].hour
    
    if hour < 12:
        return 'proceso_mantenimiento_menor'
    else:
        return 'proceso_mantenimiento_mayor'


def decide_weekday_processing(**context):
    """Segunda decisión: procesamiento de día laboral según día del mes"""
    day = context['logical_date'].day
    
    if day == 1:
        return 'proceso_cierre_mensual'
    elif day % 7 == 0:  # Múltiplos de 7
        return 'proceso_semanal'
    else:
        return 'proceso_diario'


with DAG(
    dag_id='branching_nested_decisions',
    schedule='@hourly',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'branching']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    # Primera capa de decisión: día de semana vs fin de semana
    first_branch = BranchPythonOperator(
        task_id='decidir_tipo_dia',
        python_callable=first_decision,
    )
    
    # Análisis para fin de semana
    analizar_fds = BashOperator(
        task_id='analizar_fin_semana',
        bash_command='echo "Analizando contexto de fin de semana"'
    )
    
    # Análisis para día laboral
    analizar_laboral = BashOperator(
        task_id='analizar_dia_laboral',
        bash_command='echo "Analizando contexto de día laboral"'
    )
    
    # Segunda capa de decisión: para fin de semana
    branch_weekend = BranchPythonOperator(
        task_id='decidir_proceso_fds',
        python_callable=decide_weekend_processing,
    )
    
    # Segunda capa de decisión: para día laboral
    branch_weekday = BranchPythonOperator(
        task_id='decidir_proceso_laboral',
        python_callable=decide_weekday_processing,
    )
    
    # Procesos de fin de semana
    mantenimiento_menor = BashOperator(
        task_id='proceso_mantenimiento_menor',
        bash_command='echo "Mantenimiento menor: limpieza de logs, optimización"'
    )
    
    mantenimiento_mayor = BashOperator(
        task_id='proceso_mantenimiento_mayor',
        bash_command='echo "Mantenimiento mayor: reindexación completa, backups"'
    )
    
    # Procesos de día laboral
    cierre_mensual = BashOperator(
        task_id='proceso_cierre_mensual',
        bash_command='echo "Proceso especial: cierre de mes completo"'
    )
    
    proceso_semanal = BashOperator(
        task_id='proceso_semanal',
        bash_command='echo "Proceso semanal: consolidación y reportes"'
    )
    
    proceso_diario = BashOperator(
        task_id='proceso_diario',
        bash_command='echo "Proceso diario: actualización incremental"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    # Estructura de dependencias - decisiones anidadas
    inicio >> first_branch
    
    # Rama de fin de semana
    first_branch >> analizar_fds >> branch_weekend
    branch_weekend >> [mantenimiento_menor, mantenimiento_mayor] >> fin
    
    # Rama de día laboral
    first_branch >> analizar_laboral >> branch_weekday
    branch_weekday >> [cierre_mensual, proceso_semanal, proceso_diario] >> fin
