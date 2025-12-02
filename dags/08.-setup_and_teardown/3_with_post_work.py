"""
Setup and Teardown - Anidado con Trabajo Adicional

Demuestra que después de un teardown se pueden ejecutar tareas adicionales,
y cómo combinar setup/teardown con flujos de trabajo más complejos.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='setup_teardown_with_post_work',
    start_date=datetime.datetime(2021, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['example', 'setup_and_teardown']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Setup principal
    setup = BashOperator(
        task_id='setup_resources',
        bash_command='echo "🔧 Preparando recursos principales"'
    )
    
    # Trabajo principal
    work_1 = BashOperator(
        task_id='main_processing',
        bash_command='echo "⚙️ Procesamiento principal"'
    )
    
    work_2 = BashOperator(
        task_id='secondary_processing',
        bash_command='echo "⚙️ Procesamiento secundario"'
    )
    
    # Teardown principal
    teardown = BashOperator(
        task_id='teardown_resources',
        bash_command='echo "🧹 Limpiando recursos principales"'
    ).as_teardown(setups=setup)
    
    # Trabajo DESPUÉS del teardown
    # Esto es válido y útil para tareas de post-procesamiento
    generate_report = BashOperator(
        task_id='generate_summary_report',
        bash_command='echo "📊 Generando reporte de ejecución"'
    )
    
    send_notification = BashOperator(
        task_id='send_completion_notification',
        bash_command='echo "📧 Enviando notificación de finalización"'
    )
    
    archive_logs = BashOperator(
        task_id='archive_execution_logs',
        bash_command='echo "📦 Archivando logs de ejecución"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flujo complejo:
    # 1. Inicio
    # 2. Setup
    # 3. Trabajo principal (paralelo)
    # 4. Teardown (se ejecuta después del trabajo)
    # 5. Tareas post-teardown (paralelo)
    # 6. Fin
    
    start >> setup >> [work_1, work_2] >> teardown
    teardown >> [generate_report, send_notification, archive_logs] >> end
