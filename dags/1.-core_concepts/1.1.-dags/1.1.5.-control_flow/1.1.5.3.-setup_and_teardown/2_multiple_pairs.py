"""
Setup and Teardown - Múltiples Pares Setup/Teardown

Demuestra cómo usar múltiples pares de setup/teardown independientes
para gestionar diferentes recursos en el mismo DAG.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id='setup_teardown_multiple_pairs',
    start_date=datetime.datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'setup_and_teardown']
) as dag:
    
    # Par 1: Base de datos
    setup_db = BashOperator(
        task_id='setup_database',
        bash_command='echo "💾 Configurando conexión a base de datos"'
    )
    
    query_db = BashOperator(
        task_id='query_database',
        bash_command='echo "🔍 Consultando datos de la base de datos"'
    )
    
    teardown_db = BashOperator(
        task_id='teardown_database',
        bash_command='echo "💾 Cerrando conexión a base de datos"'
    ).as_teardown(setups=setup_db)
    
    # Par 2: Sistema de archivos
    setup_fs = BashOperator(
        task_id='setup_filesystem',
        bash_command='echo "📁 Creando directorios temporales"'
    )
    
    write_files = BashOperator(
        task_id='write_temp_files',
        bash_command='echo "📝 Escribiendo archivos temporales"'
    )
    
    teardown_fs = BashOperator(
        task_id='teardown_filesystem',
        bash_command='echo "📁 Eliminando directorios temporales"'
    ).as_teardown(setups=setup_fs)
    
    # Par 3: Servicio externo
    setup_api = BashOperator(
        task_id='setup_api_client',
        bash_command='echo "🌐 Inicializando cliente API con autenticación"'
    )
    
    call_api = BashOperator(
        task_id='call_external_api',
        bash_command='echo "📡 Llamando API externa"'
    )
    
    teardown_api = BashOperator(
        task_id='teardown_api_client',
        bash_command='echo "🌐 Cerrando sesión API y limpiando tokens"'
    ).as_teardown(setups=setup_api)
    
    # Combinar resultados
    combine = BashOperator(
        task_id='combine_all_data',
        bash_command='echo "🔗 Combinando datos de todas las fuentes"'
    )
    
    # Flujos independientes que convergen
    setup_db >> query_db >> teardown_db >> combine
    setup_fs >> write_files >> teardown_fs >> combine
    setup_api >> call_api >> teardown_api >> combine
