"""
Task Groups - Grupos con Configuración

Demuestra cómo usar task groups con diferentes configuraciones
y propiedades para controlar el comportamiento visual y tooltips.
"""

import datetime

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_groups_with_config',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dag_visualization', 'task_groups']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Grupo con tooltip personalizado
    with TaskGroup(
        group_id='data_ingestion',
        tooltip='Ingestión de datos desde múltiples fuentes'
    ) as ingestion:
        
        ingest_db = BashOperator(
            task_id='from_database',
            bash_command='echo "📥 Ingesta desde PostgreSQL"'
        )
        
        ingest_api = BashOperator(
            task_id='from_rest_api',
            bash_command='echo "📥 Ingesta desde API REST"'
        )
        
        ingest_s3 = BashOperator(
            task_id='from_s3_bucket',
            bash_command='echo "📥 Ingesta desde S3"'
        )
        
        # Ingestas paralelas
        [ingest_db, ingest_api, ingest_s3]
    
    # Grupo con prefix_group_id=False
    # Los task_ids NO incluirán el group_id como prefijo
    with TaskGroup(
        group_id='quick_checks',
        prefix_group_id=False,
        tooltip='Validaciones rápidas sin prefijo en task_id'
    ) as checks:
        
        # Estos task_ids serán simplemente 'count' y 'sample', no 'quick_checks.count'
        count = BashOperator(
            task_id='count_records',
            bash_command='echo "🔢 Contando registros"'
        )
        
        sample = BashOperator(
            task_id='sample_data',
            bash_command='echo "🎲 Muestreando datos"'
        )
        
        [count, sample]
    
    # Grupo con configuración completa
    with TaskGroup(
        group_id='data_quality',
        tooltip='Suite completa de validación de calidad de datos',
        prefix_group_id=True  # Explícito (es el default)
    ) as quality:
        
        validate_nulls = BashOperator(
            task_id='check_null_values',
            bash_command='echo "✅ Verificando valores nulos"'
        )
        
        validate_duplicates = BashOperator(
            task_id='check_duplicates',
            bash_command='echo "✅ Verificando duplicados"'
        )
        
        validate_ranges = BashOperator(
            task_id='check_value_ranges',
            bash_command='echo "✅ Verificando rangos de valores"'
        )
        
        validate_references = BashOperator(
            task_id='check_referential_integrity',
            bash_command='echo "✅ Verificando integridad referencial"'
        )
        
        # Validaciones secuenciales (cada una depende de la anterior)
        validate_nulls >> validate_duplicates >> validate_ranges >> validate_references
    
    # Grupo de transformaciones
    with TaskGroup(
        group_id='transformations',
        tooltip='Transformaciones y enriquecimiento de datos'
    ) as transform:
        
        normalize = BashOperator(
            task_id='normalize_values',
            bash_command='echo "📐 Normalizando valores"'
        )
        
        denormalize = BashOperator(
            task_id='create_wide_table',
            bash_command='echo "📊 Creando tabla desnormalizada"'
        )
        
        normalize >> denormalize
    
    end = EmptyOperator(task_id='end')
    
    # Flujo principal
    start >> ingestion >> checks >> quality >> transform >> end
