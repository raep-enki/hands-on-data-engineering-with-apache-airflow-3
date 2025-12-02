"""
Setup and Teardown - Múltiples Tareas de Trabajo

Demuestra cómo un setup/teardown puede proteger múltiples tareas
de trabajo que se ejecutan en paralelo.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='setup_teardown_parallel_work',
    start_date=datetime.datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'setup_and_teardown']
) as dag:
    
    # Setup: Crear recursos compartidos
    setup = BashOperator(
        task_id='setup_environment',
        bash_command='echo "🔧 Configurando: crear schema, conectar DB, reservar recursos"'
    )
    
    # Múltiples tareas de trabajo en paralelo
    process_customers = BashOperator(
        task_id='process_customers',
        bash_command='echo "👥 Procesando datos de clientes"'
    )
    
    process_orders = BashOperator(
        task_id='process_orders',
        bash_command='echo "📦 Procesando datos de órdenes"'
    )
    
    process_products = BashOperator(
        task_id='process_products',
        bash_command='echo "🏷️ Procesando datos de productos"'
    )
    
    # Consolidar resultados
    consolidate = BashOperator(
        task_id='consolidate_results',
        bash_command='echo "📊 Consolidando todos los resultados"'
    )
    
    # Teardown: Limpiar todos los recursos
    teardown = BashOperator(
        task_id='cleanup_environment',
        bash_command='echo "🧹 Limpiando: eliminar schema, cerrar conexiones, liberar recursos"'
    ).as_teardown(setups=setup)
    
    # Flujo: setup >> trabajos paralelos >> consolidar >> teardown
    setup >> [process_customers, process_orders, process_products] >> consolidate >> teardown
