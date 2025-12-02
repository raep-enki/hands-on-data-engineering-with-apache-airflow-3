"""
Task Groups - Grupos Anidados

Demuestra cómo anidar task groups dentro de otros task groups
para crear jerarquías de organización más complejas.
"""

import datetime

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_groups_nested',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'task_groups']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Grupo principal: ETL Pipeline
    with TaskGroup(group_id='etl_pipeline') as etl:
        
        # Subgrupo: Extraction
        with TaskGroup(group_id='extraction') as extract_group:
            extract_db = BashOperator(
                task_id='from_database',
                bash_command='echo "📥 Extrayendo de base de datos"'
            )
            
            extract_api = BashOperator(
                task_id='from_api',
                bash_command='echo "📥 Extrayendo de API"'
            )
            
            extract_files = BashOperator(
                task_id='from_files',
                bash_command='echo "📥 Extrayendo de archivos"'
            )
        
        # Subgrupo: Transformation
        with TaskGroup(group_id='transformation') as transform_group:
            clean = BashOperator(
                task_id='clean_data',
                bash_command='echo "🧹 Limpiando datos"'
            )
            
            enrich = BashOperator(
                task_id='enrich_data',
                bash_command='echo "✨ Enriqueciendo datos"'
            )
            
            aggregate = BashOperator(
                task_id='aggregate_data',
                bash_command='echo "📊 Agregando datos"'
            )
            
            clean >> enrich >> aggregate
        
        # Subgrupo: Loading
        with TaskGroup(group_id='loading') as load_group:
            load_warehouse = BashOperator(
                task_id='to_warehouse',
                bash_command='echo "📤 Cargando a warehouse"'
            )
            
            load_cache = BashOperator(
                task_id='to_cache',
                bash_command='echo "📤 Cargando a cache"'
            )
            
            # Cargas en paralelo
            [load_warehouse, load_cache]
        
        # Flujo dentro del grupo ETL
        extract_group >> transform_group >> load_group
    
    # Grupo: Quality Checks
    with TaskGroup(group_id='quality_checks') as quality:
        check_completeness = BashOperator(
            task_id='check_completeness',
            bash_command='echo "✅ Verificando completitud"'
        )
        
        check_accuracy = BashOperator(
            task_id='check_accuracy',
            bash_command='echo "✅ Verificando precisión"'
        )
        
        [check_completeness, check_accuracy]
    
    end = EmptyOperator(task_id='end')
    
    # Flujo principal
    start >> etl >> quality >> end
