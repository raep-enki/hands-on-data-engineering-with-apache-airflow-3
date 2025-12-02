"""
Deadline Alerts - Data Quality Checks

Demuestra un deadline para checks de calidad de datos.
Los checks deben ejecutarse rápido para detectar problemas
oportunamente y alertar a los equipos.

Pipeline que se ejecuta cada 3 horas con múltiples
validaciones en paralelo.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, AsyncCallback
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


async def alert_callback(**context):
    """Función async que se ejecuta cuando se alcanza el deadline."""
    print(f"⚠️ DEADLINE ALCANZADO - Quality checks retrasados en DAG: {context.get('dag_id')}")


with DAG(
    dag_id='deadline_alerts_data_quality',
    schedule='0 */3 * * *',  # cada 3 horas
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'deadline_alerts'],
    # Deadline: 30 minutos (checks deben ser rápidos)
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(minutes=30),
        callback=AsyncCallback(callback_callable=alert_callback)
    )
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    check_freshness = BashOperator(
        task_id='check_data_freshness',
        bash_command='echo "🕐 Verificando frescura de datos"'
    )
    
    check_completeness = BashOperator(
        task_id='check_completeness',
        bash_command='echo "📋 Verificando completitud"'
    )
    
    check_accuracy = BashOperator(
        task_id='check_accuracy',
        bash_command='echo "🎯 Verificando precisión"'
    )
    
    check_consistency = BashOperator(
        task_id='check_consistency',
        bash_command='echo "🔄 Verificando consistencia"'
    )
    
    generate_report = BashOperator(
        task_id='generate_quality_report',
        bash_command='echo "📄 Generando reporte de calidad"'
    )
    
    alert_if_issues = BashOperator(
        task_id='alert_on_issues',
        bash_command='echo "🚨 Alertando si hay problemas"'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> [check_freshness, check_completeness, check_accuracy, check_consistency]
    [check_freshness, check_completeness, check_accuracy, check_consistency] >> generate_report
    generate_report >> alert_if_issues >> end
