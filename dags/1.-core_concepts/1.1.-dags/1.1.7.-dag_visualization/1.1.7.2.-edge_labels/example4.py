"""
Edge Labels - Labels para Flujos Paralelos

Demuestra cómo usar labels para documentar procesamiento paralelo
y la consolidación de resultados de múltiples tareas.
"""

import datetime

from airflow.sdk import DAG, Label
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='edge_labels_parallel_flows',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dag_visualization', 'edge_labels']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    prepare = BashOperator(
        task_id='prepare_environment',
        bash_command='echo "🔧 Preparando entorno"'
    )
    
    # Procesamiento paralelo de diferentes fuentes
    source_a = BashOperator(
        task_id='process_source_a',
        bash_command='echo "📊 Procesando fuente A (SQL)"'
    )
    
    source_b = BashOperator(
        task_id='process_source_b',
        bash_command='echo "📊 Procesando fuente B (NoSQL)"'
    )
    
    source_c = BashOperator(
        task_id='process_source_c',
        bash_command='echo "📊 Procesando fuente C (API)"'
    )
    
    source_d = BashOperator(
        task_id='process_source_d',
        bash_command='echo "📊 Procesando fuente D (Files)"'
    )
    
    # Consolidación
    merge = BashOperator(
        task_id='merge_all_sources',
        bash_command='echo "🔗 Consolidando todas las fuentes"'
    )
    
    validate = BashOperator(
        task_id='validate_merged_data',
        bash_command='echo "✅ Validando datos consolidados"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flujo con labels documentando cada fuente
    start >> Label('Preparar') >> prepare
    
    # Labels específicos para cada fuente paralela
    prepare >> Label('Procesar SQL') >> source_a >> Label('Datos SQL') >> merge
    prepare >> Label('Procesar NoSQL') >> source_b >> Label('Datos NoSQL') >> merge
    prepare >> Label('Procesar API') >> source_c >> Label('Datos API') >> merge
    prepare >> Label('Procesar Archivos') >> source_d >> Label('Datos Archivos') >> merge
    
    # Consolidación y finalización
    merge >> Label('Datos consolidados') >> validate
    validate >> Label('Validación OK') >> end
