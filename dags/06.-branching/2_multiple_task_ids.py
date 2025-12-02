"""
Branching con Retorno de Múltiples Task IDs

Demuestra cómo una función de branch puede retornar múltiples task_ids
para ejecutar varias tareas en paralelo después de la decisión.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def choose_processing_tasks(**context):
    """
    Decide qué conjunto de tareas ejecutar.
    Puede retornar un solo task_id o una lista de task_ids.
    """
    logical_date = context['logical_date']
    day = logical_date.day
    
    # Primer día del mes: ejecutar todas las validaciones
    if day == 1:
        return ['validar_calidad', 'validar_integridad', 'validar_duplicados']
    # Días 5, 10, 15, 20, 25: validación media
    elif day % 5 == 0:
        return ['validar_calidad', 'validar_integridad']
    # Resto de días: validación básica
    else:
        return 'validar_calidad'


with DAG(
    dag_id='branching_multiple_tasks',
    start_date=datetime.datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'branching']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    extraer = BashOperator(
        task_id='extraer_datos',
        bash_command='echo "Extrayendo datos de origen"'
    )
    
    branch = BranchPythonOperator(
        task_id='decidir_validaciones',
        python_callable=choose_processing_tasks,
    )
    
    # Diferentes niveles de validación
    validar_calidad = BashOperator(
        task_id='validar_calidad',
        bash_command='echo "Validando calidad de datos (básico)"'
    )
    
    validar_integridad = BashOperator(
        task_id='validar_integridad',
        bash_command='echo "Validando integridad referencial"'
    )
    
    validar_duplicados = BashOperator(
        task_id='validar_duplicados',
        bash_command='echo "Detectando y eliminando duplicados"'
    )
    
    procesar = BashOperator(
        task_id='procesar_datos',
        bash_command='echo "Procesando datos validados"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    # Estructura de dependencias
    inicio >> extraer >> branch
    branch >> [validar_calidad, validar_integridad, validar_duplicados]
    [validar_calidad, validar_integridad, validar_duplicados] >> procesar >> fin
