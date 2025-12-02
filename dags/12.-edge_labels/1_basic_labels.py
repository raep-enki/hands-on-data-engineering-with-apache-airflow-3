"""
Edge Labels - Etiquetas Básicas

Demuestra cómo usar Label para añadir texto descriptivo a las
conexiones entre tareas, mejorando la comprensión del flujo del DAG.
"""

import datetime

from airflow.sdk import DAG, Label
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='edge_labels_basic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'edge_labels']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "📥 Extrayendo datos"'
    )
    
    validate = BashOperator(
        task_id='validate_data',
        bash_command='echo "✅ Validando datos"'
    )
    
    transform = BashOperator(
        task_id='transform_data',
        bash_command='echo "⚙️ Transformando datos"'
    )
    
    load = BashOperator(
        task_id='load_data',
        bash_command='echo "📤 Cargando datos"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flujo con labels descriptivos
    start >> Label('Iniciar procesamiento') >> extract
    extract >> Label('Datos crudos') >> validate
    validate >> Label('Datos validados') >> transform
    transform >> Label('Datos transformados') >> load
    load >> Label('Completado') >> end
