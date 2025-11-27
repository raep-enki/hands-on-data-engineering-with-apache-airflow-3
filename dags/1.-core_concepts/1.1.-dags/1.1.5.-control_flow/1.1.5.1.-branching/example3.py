"""
Branching con Múltiples Ramas Condicionales

Demuestra cómo usar múltiples condiciones para decidir entre varias rutas de procesamiento.
Este ejemplo simula diferentes estrategias según el día del mes.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def choose_processing_strategy(**context):
    """
    Decide la estrategia de procesamiento según el día del mes.
    """
    logical_date = context['logical_date']
    day = logical_date.day
    
    if day <= 10:
        return 'procesamiento_completo'
    elif day <= 20:
        return 'procesamiento_incremental'
    else:
        return 'procesamiento_rapido'


with DAG(
    dag_id='branching_multiple_conditions',
    start_date=datetime.datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'branching']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    extraer = BashOperator(
        task_id='extraer_datos',
        bash_command='echo "Extrayendo datos de origen"'
    )
    
    branch = BranchPythonOperator(
        task_id='elegir_estrategia',
        python_callable=choose_processing_strategy,
    )
    
    # Diferentes estrategias de procesamiento
    completo = BashOperator(
        task_id='procesamiento_completo',
        bash_command='echo "Procesamiento completo: validación exhaustiva + transformaciones completas"'
    )
    
    incremental = BashOperator(
        task_id='procesamiento_incremental',
        bash_command='echo "Procesamiento incremental: solo cambios desde última ejecución"'
    )
    
    rapido = BashOperator(
        task_id='procesamiento_rapido',
        bash_command='echo "Procesamiento rápido: validación básica + transformaciones mínimas"'
    )
    
    cargar = BashOperator(
        task_id='cargar_resultados',
        bash_command='echo "Cargando resultados al data warehouse"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    # Estructura de dependencias
    inicio >> extraer >> branch >> [completo, incremental, rapido] >> cargar >> fin
