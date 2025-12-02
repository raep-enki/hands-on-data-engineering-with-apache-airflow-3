"""
Dynamic DAGs - Pipeline Paralelo por País

Demuestra cómo crear múltiples pipelines completos de forma dinámica,
uno para cada entidad (país, cliente, producto, etc.).
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='dynamic_parallel_pipelines',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dynamic_dags']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Lista de países a procesar
    countries = ['usa', 'canada', 'mexico', 'brazil', 'argentina']
    
    # Crear un pipeline completo para cada país
    for country in countries:
        # Extracción
        extract = BashOperator(
            task_id=f'extract_{country}',
            bash_command=f'echo "📥 Extrayendo datos de {country.upper()}"'
        )
        
        # Transformación
        transform = BashOperator(
            task_id=f'transform_{country}',
            bash_command=f'echo "⚙️ Transformando datos de {country.upper()}"'
        )
        
        # Carga
        load = BashOperator(
            task_id=f'load_{country}',
            bash_command=f'echo "📤 Cargando datos de {country.upper()}"'
        )
        
        # Pipeline: start >> extract >> transform >> load
        start >> extract >> transform >> load
    
    # Consolidar todos los países
    consolidate = BashOperator(
        task_id='consolidate_all_countries',
        bash_command='echo "🌎 Consolidando datos de todos los países"'
    )
    
    # Conectar todas las cargas a la consolidación
    for country in countries:
        dag.get_task(f'load_{country}') >> consolidate
    
    end = EmptyOperator(task_id='end')
    consolidate >> end
