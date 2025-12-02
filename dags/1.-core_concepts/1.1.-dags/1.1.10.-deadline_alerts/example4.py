"""
Deadline Alerts - E-commerce Order Processing

Demuestra un deadline para procesamiento de órdenes en e-commerce.
Las órdenes deben procesarse rápido para garantizar fulfillment oportuno.

Pipeline crítico que se ejecuta cada hora y debe completarse
en 45 minutos para no afectar el proceso de envíos.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, AsyncCallback
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


async def alert_callback(**context):
    """Función async que se ejecuta cuando se alcanza el deadline."""
    print(f"⚠️ DEADLINE ALCANZADO - Órdenes retrasadas en DAG: {context.get('dag_id')}")


with DAG(
    dag_id='deadline_alerts_ecommerce_orders',
    schedule='@hourly',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'deadline_alerts'],
    # Deadline: 45 minutos (las órdenes deben procesarse rápido)
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=45),
        callback=AsyncCallback(callback_callable=alert_callback)
    )
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract_orders = BashOperator(
        task_id='extract_new_orders',
        bash_command='echo "🛒 Extrayendo nuevas órdenes"'
    )
    
    validate_payment = BashOperator(
        task_id='validate_payment',
        bash_command='echo "💳 Validando pagos"'
    )
    
    check_inventory = BashOperator(
        task_id='check_inventory',
        bash_command='echo "📦 Verificando inventario"'
    )
    
    create_shipment = BashOperator(
        task_id='create_shipment',
        bash_command='echo "🚚 Creando orden de envío"'
    )
    
    notify_customer = BashOperator(
        task_id='notify_customer',
        bash_command='echo "📧 Notificando confirmación al cliente"'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> extract_orders >> validate_payment >> check_inventory >> create_shipment >> notify_customer >> end
