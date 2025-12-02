"""
Edge Labels - Labels para Documentar Transformaciones

Demuestra cómo usar labels para documentar qué transformaciones
o cambios se aplican a los datos en cada paso del pipeline.
"""

import datetime

from airflow.sdk import DAG, Label
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='edge_labels_transformations',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dag_visualization', 'edge_labels']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Extracción
    extract = BashOperator(
        task_id='extract_raw_data',
        bash_command='echo "📥 Extrayendo datos crudos"'
    )
    
    # Serie de transformaciones con labels descriptivos
    remove_nulls = BashOperator(
        task_id='remove_null_values',
        bash_command='echo "🧹 Removiendo valores nulos"'
    )
    
    deduplicate = BashOperator(
        task_id='remove_duplicates',
        bash_command='echo "🧹 Removiendo duplicados"'
    )
    
    normalize = BashOperator(
        task_id='normalize_fields',
        bash_command='echo "📐 Normalizando campos"'
    )
    
    enrich = BashOperator(
        task_id='enrich_with_lookups',
        bash_command='echo "✨ Enriqueciendo con lookups"'
    )
    
    aggregate = BashOperator(
        task_id='aggregate_metrics',
        bash_command='echo "📊 Agregando métricas"'
    )
    
    format_output = BashOperator(
        task_id='format_for_warehouse',
        bash_command='echo "📦 Formateando para warehouse"'
    )
    
    load = BashOperator(
        task_id='load_to_warehouse',
        bash_command='echo "📤 Cargando a warehouse"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flujo con labels detallando cada transformación
    start >> Label('Datos fuente') >> extract
    extract >> Label('Raw: 10M filas, nulls, duplicados') >> remove_nulls
    remove_nulls >> Label('9.5M filas, sin nulls') >> deduplicate
    deduplicate >> Label('8M filas únicas') >> normalize
    normalize >> Label('Campos estandarizados') >> enrich
    enrich >> Label('+ datos de referencia') >> aggregate
    aggregate >> Label('Agregado por día/región') >> format_output
    format_output >> Label('Formato Parquet') >> load
    load >> Label('Completado') >> end
