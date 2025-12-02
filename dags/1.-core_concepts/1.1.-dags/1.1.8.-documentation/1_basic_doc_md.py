"""
# DAG Documentation - Básico

Este DAG demuestra cómo agregar documentación a un DAG usando docstrings
y la propiedad `doc_md`.

La documentación aparece en la UI de Airflow y ayuda a otros desarrolladores
a entender el propósito y funcionamiento del DAG.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='documentation_basic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'documentation']
) as dag:
    # Asignar el docstring del módulo como documentación del DAG
    dag.doc_md = __doc__
    
    start = EmptyOperator(task_id='start')
    
    process = BashOperator(
        task_id='process_data',
        bash_command='echo "⚙️ Procesando datos"'
    )
    # Documentación específica para esta tarea
    process.doc_md = """
    ## Process Data Task
    
    Esta tarea procesa los datos extraídos aplicando las siguientes transformaciones:
    - Limpieza de valores nulos
    - Normalización de campos
    - Validación de esquema
    
    **Input**: Datos crudos en formato JSON
    **Output**: Datos procesados en formato Parquet
    """
    
    end = EmptyOperator(task_id='end')
    
    start >> process >> end
