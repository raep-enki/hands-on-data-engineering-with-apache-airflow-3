"""
Setup and Teardown Pattern - Ejemplo Básico

Demuestra el patrón setup/teardown más simple: una tarea de setup
que prepara recursos y una tarea de teardown que los limpia.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='setup_teardown_basic',
    start_date=datetime.datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'setup_and_teardown']
) as dag:
    
    # Setup: Preparar el entorno
    setup = BashOperator(
        task_id='create_temp_table',
        bash_command='echo "📋 Creando tabla temporal en la base de datos"'
    )
    
    # Tarea de trabajo
    work = BashOperator(
        task_id='process_data',
        bash_command='echo "⚙️ Procesando datos en la tabla temporal"'
    )
    
    # Teardown: Limpiar recursos
    # El método .as_teardown() garantiza que esta tarea se ejecute
    # incluso si 'work' falla
    teardown = BashOperator(
        task_id='drop_temp_table',
        bash_command='echo "🧹 Eliminando tabla temporal"'
    ).as_teardown(setups=setup)
    
    # Flujo: setup debe completarse antes del trabajo,
    # y teardown se ejecuta después del trabajo (exitoso o no)
    setup >> work >> teardown
