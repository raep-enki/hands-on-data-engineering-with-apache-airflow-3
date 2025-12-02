"""
Task Groups - Organización Básica

Demuestra cómo usar TaskGroup para agrupar tareas relacionadas
visualmente en el DAG, mejorando la legibilidad del grafo.

NOTA: TaskGroup es una herramienta de VISUALIZACIÓN, no cambia
la lógica de ejecución, solo organiza el grafo del DAG.
"""

import datetime

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_groups_basic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'task_groups']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Grupo 1: Procesamiento de datos
    with TaskGroup(group_id='data_processing') as data_group:
        extract = BashOperator(
            task_id='extract',
            bash_command='echo "📥 Extrayendo datos"'
        )
        
        transform = BashOperator(
            task_id='transform',
            bash_command='echo "⚙️ Transformando datos"'
        )
        
        load = BashOperator(
            task_id='load',
            bash_command='echo "📤 Cargando datos"'
        )
        
        extract >> transform >> load
    
    # Grupo 2: Validaciones
    with TaskGroup(group_id='validations') as validation_group:
        validate_quality = BashOperator(
            task_id='validate_quality',
            bash_command='echo "✅ Validando calidad"'
        )
        
        validate_schema = BashOperator(
            task_id='validate_schema',
            bash_command='echo "✅ Validando esquema"'
        )
        
        # Validaciones en paralelo
        [validate_quality, validate_schema]
    
    end = EmptyOperator(task_id='end')
    
    # Flujo: los grupos se tratan como una sola unidad
    start >> data_group >> validation_group >> end
