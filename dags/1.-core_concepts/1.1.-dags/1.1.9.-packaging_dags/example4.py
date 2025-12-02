"""
DAG Packaging - Organización por Subdirectorios

Demuestra cómo organizar DAGs en subdirectorios temáticos,
útil para proyectos grandes con muchos DAGs.

Estructura recomendada:
dags/
  ├── sales/
  │   ├── __init__.py
  │   ├── daily_sales_etl.py
  │   └── monthly_sales_report.py
  ├── marketing/
  │   ├── __init__.py
  │   ├── campaign_sync.py
  │   └── email_metrics.py
  └── utils/
      ├── __init__.py
      ├── validators.py
      └── notifications.py

Este archivo simula estar en dags/sales/
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Simulación de imports desde módulos compartidos
# En producción: from dags.utils.validators import validate_sales_data
# En producción: from dags.utils.notifications import send_team_notification

def validate_sales_data(**context):
    """Valida datos de ventas (estaría en dags/utils/validators.py)"""
    print("✅ Validando integridad de datos de ventas")
    return True


def send_team_notification(message, **context):
    """Envía notificación (estaría en dags/utils/notifications.py)"""
    print(f"📧 Notificación: {message}")
    return True


# Configuración específica del dominio de ventas
# En producción: from dags.sales.config import SALES_CONFIG
SALES_CONFIG = {
    'source_db': 'postgres_sales',
    'target_table': 'analytics.daily_sales',
    'notification_channel': '#sales-team',
    'sla_hours': 2
}


with DAG(
    dag_id='packaging_subdirectory_sales',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'packaging_dags', 'sales']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract = BashOperator(
        task_id='extract_sales',
        bash_command=f'echo "📥 Extrayendo de {SALES_CONFIG["source_db"]}"'
    )
    
    # Usar función de validación compartida
    validate = PythonOperator(
        task_id='validate_sales',
        python_callable=validate_sales_data
    )
    
    transform = BashOperator(
        task_id='transform_sales',
        bash_command='echo "⚙️ Transformando datos de ventas"'
    )
    
    load = BashOperator(
        task_id='load_to_warehouse',
        bash_command=f'echo "📤 Cargando a {SALES_CONFIG["target_table"]}"'
    )
    
    # Usar función de notificación compartida
    notify = PythonOperator(
        task_id='notify_team',
        python_callable=send_team_notification,
        op_kwargs={'message': f'Pipeline de ventas completado - {SALES_CONFIG["notification_channel"]}'}
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> extract >> validate >> transform >> load >> notify >> end
