"""
Setup and Teardown - Setup con Múltiples Teardowns

Demuestra cómo un único setup puede tener múltiples teardowns,
útil cuando se necesita limpiar diferentes aspectos de los recursos.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='setup_teardown_multiple_cleanups',
    start_date=datetime.datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'setup_and_teardown']
) as dag:
    
    # Setup único: Crear entorno completo
    setup = BashOperator(
        task_id='setup_complete_environment',
        bash_command='echo "🚀 Configurando entorno: DB + Cache + Storage + Logging"'
    )
    
    # Tareas de trabajo
    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "📥 Extrayendo datos"'
    )
    
    transform = BashOperator(
        task_id='transform_data',
        bash_command='echo "⚙️ Transformando datos"'
    )
    
    load = BashOperator(
        task_id='load_data',
        bash_command='echo "📤 Cargando datos"'
    )
    
    # Múltiples teardowns para diferentes aspectos de la limpieza
    cleanup_cache = BashOperator(
        task_id='cleanup_cache',
        bash_command='echo "🗑️ Limpiando cache y archivos temporales"'
    ).as_teardown(setups=setup)
    
    cleanup_connections = BashOperator(
        task_id='cleanup_connections',
        bash_command='echo "🔌 Cerrando todas las conexiones de red"'
    ).as_teardown(setups=setup)
    
    cleanup_storage = BashOperator(
        task_id='cleanup_storage',
        bash_command='echo "💾 Liberando espacio de almacenamiento"'
    ).as_teardown(setups=setup)
    
    cleanup_logs = BashOperator(
        task_id='cleanup_logs',
        bash_command='echo "📋 Archivando y rotando logs"'
    ).as_teardown(setups=setup)
    
    # Verificación final después de toda la limpieza
    verify = EmptyOperator(task_id='verify_cleanup')
    
    # Flujo: setup >> ETL >> múltiples limpiezas en paralelo >> verificación
    setup >> extract >> transform >> load
    load >> [cleanup_cache, cleanup_connections, cleanup_storage, cleanup_logs] >> verify
