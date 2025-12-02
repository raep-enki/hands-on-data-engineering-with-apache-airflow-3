"""
DAG Packaging - Configuración Compartida

Demuestra cómo usar archivos de configuración para compartir
constantes, default_args y otras configuraciones entre DAGs.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Simulación de un módulo de configuración compartida
# En producción: from dags.config.common import COMMON_DEFAULT_ARGS, TAGS_CONFIG

COMMON_DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'execution_timeout': datetime.timedelta(hours=2)
}

TAGS_CONFIG = {
    'environment': 'production',
    'team': 'data_engineering',
    'domain': 'sales'
}

# Configuración de conexiones (en producción vendría de Variables o config file)
DATA_SOURCES = {
    'postgres': {
        'conn_id': 'postgres_sales',
        'schema': 'public',
        'table': 'sales_transactions'
    },
    's3': {
        'bucket': 'company-data-lake',
        'prefix': 'raw/sales/'
    }
}


with DAG(
    dag_id='packaging_shared_config',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    default_args=COMMON_DEFAULT_ARGS,
    tags=['example', 'packaging_dags']]
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Usar configuración compartida en los comandos
    extract_postgres = BashOperator(
        task_id='extract_from_postgres',
        bash_command=f'echo "📥 Extrayendo de {DATA_SOURCES["postgres"]["conn_id"]}"'
    )
    
    extract_s3 = BashOperator(
        task_id='extract_from_s3',
        bash_command=f'echo "📥 Extrayendo de s3://{DATA_SOURCES["s3"]["bucket"]}/{DATA_SOURCES["s3"]["prefix"]}"'
    )
    
    merge = BashOperator(
        task_id='merge_sources',
        bash_command='echo "🔗 Consolidando fuentes"'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> [extract_postgres, extract_s3] >> merge >> end
