"""
DAG Packaging - Factory Pattern para DAGs Similares

Demuestra cómo usar factory functions para crear múltiples DAGs
similares con diferentes configuraciones, reduciendo código duplicado.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def create_etl_dag(dag_id, source_name, schedule, table_name, priority='medium'):
    """
    Factory function para crear DAGs ETL estandarizados.
    
    Args:
        dag_id: ID único del DAG
        source_name: Nombre de la fuente de datos
        schedule: Schedule interval
        table_name: Tabla destino
        priority: Prioridad del procesamiento
    """
    
    default_args = {
        'owner': f'{source_name}_owner',
        'retries': 3 if priority == 'high' else 2
    }
    
    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=datetime.datetime(2021, 1, 1),
        catchup=False,
        default_args=default_args,
        tags=['example', 'packaging_dags']
    ) as dag:
        
        start = EmptyOperator(task_id='start')
        
        extract = BashOperator(
            task_id='extract',
            bash_command=f'echo "📥 Extrayendo datos de {source_name}"'
        )
        
        validate = BashOperator(
            task_id='validate',
            bash_command=f'echo "✅ Validando datos de {source_name}"'
        )
        
        transform = BashOperator(
            task_id='transform',
            bash_command=f'echo "⚙️ Transformando datos para {table_name}"'
        )
        
        load = BashOperator(
            task_id='load',
            bash_command=f'echo "📤 Cargando a tabla {table_name}"'
        )
        
        end = EmptyOperator(task_id='end')
        
        start >> extract >> validate >> transform >> load >> end
    
    return dag


# Crear múltiples DAGs usando la factory function
customers_dag = create_etl_dag(
    dag_id='packaging_factory_customers',
    source_name='CRM',
    schedule='@hourly',
    table_name='dim_customers',
    priority='high'
)

products_dag = create_etl_dag(
    dag_id='packaging_factory_products',
    source_name='ERP',
    schedule='@daily',
    table_name='dim_products',
    priority='medium'
)

orders_dag = create_etl_dag(
    dag_id='packaging_factory_orders',
    source_name='E-commerce',
    schedule='*/15 * * * *',  # cada 15 minutos
    table_name='fact_orders',
    priority='high'
)
