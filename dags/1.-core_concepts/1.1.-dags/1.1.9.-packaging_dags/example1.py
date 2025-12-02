"""
DAG Packaging - Estructura Básica con Módulos Auxiliares

Demuestra cómo organizar un DAG usando módulos auxiliares separados
para mejorar la mantenibilidad y reutilización de código.

Este patrón es útil cuando múltiples DAGs comparten lógica común.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Funciones helper que podrían estar en un módulo separado
def validate_data(**context):
    """Valida la estructura de datos"""
    print("✅ Validando datos...")
    # En producción, esto estaría en dags/utils/validators.py
    return True


def calculate_metrics(**context):
    """Calcula métricas de negocio"""
    print("📊 Calculando métricas...")
    # En producción, esto estaría en dags/utils/metrics.py
    return {"total": 1000, "avg": 25.5}


# Configuración que podría estar en un módulo de configuración
DAG_DEFAULT_ARGS = {
    'owner': 'data_team',
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5)
}


with DAG(
    dag_id='packaging_basic_structure',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS,
    tags=['example', 'core_concepts', 'dags', 'packaging_dags']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "📥 Extrayendo datos"'
    )
    
    # Usar función helper
    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )
    
    transform = BashOperator(
        task_id='transform_data',
        bash_command='echo "⚙️ Transformando datos"'
    )
    
    # Usar otra función helper
    metrics = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics
    )
    
    load = BashOperator(
        task_id='load_data',
        bash_command='echo "📤 Cargando datos"'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> extract >> validate >> transform >> metrics >> load >> end
