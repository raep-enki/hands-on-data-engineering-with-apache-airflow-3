"""
Pipeline de Validación de Datos - Estilo Instancia Explícita

Demuestra la declaración explícita de instancia DAG para mayor control.
Usa operadores básicos sin paso de datos entre tareas.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Estilo instancia explícita - útil cuando necesitas el objeto DAG para lógica compleja
dag = DAG(
    dag_id='data_validation_pipeline',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'declaring_a_dag']
)

# Todas las tareas deben referenciar explícitamente el parámetro dag
check_source = BashOperator(
    task_id='check_source',
    bash_command='echo "Verificando conectividad con fuente de datos"',
    dag=dag
)

validate_schema = BashOperator(
    task_id='validate_schema',
    bash_command='echo "Validando esquema y estructura de datos"',
    dag=dag
)

check_quality = BashOperator(
    task_id='check_quality',
    bash_command='echo "Ejecutando verificaciones de calidad de datos"',
    dag=dag
)

generate_report = BashOperator(
    task_id='generate_report',
    bash_command='echo "Generando reporte de validación"',
    dag=dag
)

notify_success = EmptyOperator(
    task_id='notify_success',
    dag=dag
)

# Dependencias de tareas lineales
check_source >> validate_schema >> check_quality >> generate_report >> notify_success
