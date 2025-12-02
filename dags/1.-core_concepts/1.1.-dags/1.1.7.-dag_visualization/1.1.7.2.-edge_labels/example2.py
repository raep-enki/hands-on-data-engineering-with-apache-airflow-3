"""
Edge Labels - Labels en Branching

Demuestra cómo usar labels para documentar decisiones de branching,
haciendo más claro qué camino se toma bajo qué condiciones.
"""

import datetime
import random

from airflow.sdk import DAG, Label
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

def decide_processing_type(**context):
    """Decide el tipo de procesamiento según la hora"""
    hour = context['logical_date'].hour
    
    if hour < 6:
        return 'light_processing'
    elif hour < 18:
        return 'heavy_processing'
    else:
        return 'batch_processing'

with DAG(
    dag_id='edge_labels_branching',
    schedule='@hourly',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dag_visualization', 'edge_labels']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    check_load = BashOperator(
        task_id='check_system_load',
        bash_command='echo "📊 Verificando carga del sistema"'
    )
    
    decide = BranchPythonOperator(
        task_id='decide_processing',
        python_callable=decide_processing_type
    )
    
    # Opciones de procesamiento
    light = BashOperator(
        task_id='light_processing',
        bash_command='echo "🟢 Procesamiento ligero (00:00-05:59)"'
    )
    
    heavy = BashOperator(
        task_id='heavy_processing',
        bash_command='echo "🟡 Procesamiento pesado (06:00-17:59)"'
    )
    
    batch = BashOperator(
        task_id='batch_processing',
        bash_command='echo "🔴 Procesamiento batch (18:00-23:59)"'
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flujo con labels explicando las decisiones
    start >> Label('Verificar carga') >> check_load
    check_load >> Label('Decidir estrategia') >> decide
    
    # Labels en cada rama explicando la condición
    decide >> Label('Si 00:00-05:59') >> light >> Label('Procesado') >> end
    decide >> Label('Si 06:00-17:59') >> heavy >> Label('Procesado') >> end
    decide >> Label('Si 18:00-23:59') >> batch >> Label('Procesado') >> end
