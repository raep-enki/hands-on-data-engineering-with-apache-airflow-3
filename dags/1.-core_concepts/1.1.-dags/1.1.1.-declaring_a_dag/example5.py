"""
Pipeline de Datos Multi-Etapa - Dependencias Complejas

Demuestra patrones de dependencias de tareas más complejos.
Muestra cómo estructurar un flujo de trabajo real de ingeniería de datos con etapas.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id='multi_stage_data_pipeline',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    description='Pipeline multi-etapa con dependencias complejas',
    tags=['example', 'core_concepts', 'dags', 'declaring_a_dag', 'complex']
):
    # Etapa 1: Inicialización
    start = EmptyOperator(task_id='start')
    
    init_workspace = BashOperator(
        task_id='init_workspace',
        bash_command='echo "Inicializando espacio de trabajo y directorios"'
    )
    
    check_prerequisites = BashOperator(
        task_id='check_prerequisites',
        bash_command='echo "Verificando prerequisitos del sistema"'
    )
    
    # Etapa 2: Ingesta de Datos (paralelo)
    ingest_customers = BashOperator(
        task_id='ingest_customers',
        bash_command='echo "Ingiriendo datos de clientes"'
    )
    
    ingest_orders = BashOperator(
        task_id='ingest_orders',
        bash_command='echo "Ingiriendo datos de pedidos"'
    )
    
    ingest_products = BashOperator(
        task_id='ingest_products',
        bash_command='echo "Ingiriendo datos de productos"'
    )
    
    # Etapa 3: Validación de Datos (paralelo)
    validate_customers = BashOperator(
        task_id='validate_customers',
        bash_command='echo "Validando calidad de datos de clientes"'
    )
    
    validate_orders = BashOperator(
        task_id='validate_orders',
        bash_command='echo "Validando calidad de datos de pedidos"'
    )
    
    validate_products = BashOperator(
        task_id='validate_products',
        bash_command='echo "Validando calidad de datos de productos"'
    )
    
    # Etapa 4: Punto de Control
    validation_checkpoint = EmptyOperator(task_id='validation_checkpoint')
    
    # Etapa 5: Enriquecimiento de Datos (paralelo)
    enrich_customers = BashOperator(
        task_id='enrich_customers',
        bash_command='echo "Enriqueciendo perfiles de clientes"'
    )
    
    enrich_orders = BashOperator(
        task_id='enrich_orders',
        bash_command='echo "Enriqueciendo detalles de pedidos"'
    )
    
    # Etapa 6: Agregación
    aggregate_data = BashOperator(
        task_id='aggregate_data',
        bash_command='echo "Agregando todas las fuentes de datos"'
    )
    
    # Etapa 7: Reportes
    generate_summary = BashOperator(
        task_id='generate_summary',
        bash_command='echo "Generando estadísticas resumen"'
    )
    
    create_dashboards = BashOperator(
        task_id='create_dashboards',
        bash_command='echo "Creando vistas de dashboard"'
    )
    
    # Etapa 8: Finalización
    cleanup_workspace = BashOperator(
        task_id='cleanup_workspace',
        bash_command='echo "Limpiando espacio de trabajo temporal"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Definir dependencias complejas de tareas
    # Etapa 1
    start >> [init_workspace, check_prerequisites]
    
    # Etapa 2: Fan-out desde múltiples tareas a múltiples tareas
    for prep_task in [init_workspace, check_prerequisites]:
        for ingest_task in [ingest_customers, ingest_orders, ingest_products]:
            prep_task >> ingest_task
    
    # Etapa 3
    ingest_customers >> validate_customers
    ingest_orders >> validate_orders
    ingest_products >> validate_products
    
    # Etapa 4
    [validate_customers, validate_orders, validate_products] >> validation_checkpoint
    
    # Etapa 5
    validation_checkpoint >> [enrich_customers, enrich_orders]
    
    # Etapa 6
    [enrich_customers, enrich_orders] >> aggregate_data
    
    # Etapa 7
    aggregate_data >> [generate_summary, create_dashboards]
    
    # Etapa 8
    [generate_summary, create_dashboards] >> cleanup_workspace >> end
