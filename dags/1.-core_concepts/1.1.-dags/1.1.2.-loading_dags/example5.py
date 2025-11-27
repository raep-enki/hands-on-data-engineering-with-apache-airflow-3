"""
Carga Condicional Basada en Entorno

Demuestra cómo cargar DAGs condicionalmente según variables de entorno.
El DAG solo se crea si se cumple la condición del entorno.
"""

import datetime
import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Obtener el entorno actual (por defecto 'development')
ENTORNO = os.getenv('AIRFLOW_ENV', 'development')

# DAG que se carga en todos los entornos pero con diferente configuración
schedule_por_entorno = {
    'production': '@hourly',
    'staging': '@daily',
    'development': None  # Solo manual
}

dag_universal = DAG(
    dag_id='pipeline_universal',
    start_date=datetime.datetime(2024, 1, 1),
    schedule=schedule_por_entorno.get(ENTORNO, None),
    catchup=False,
    description=f'Pipeline universal - Entorno: {ENTORNO}',
    tags=['example', 'core_concepts', 'dags', 'loading_dags', ENTORNO]
)

with dag_universal:
    inicio = EmptyOperator(task_id='inicio')
    
    verificar_entorno = BashOperator(
        task_id='verificar_entorno',
        bash_command=f'echo "Ejecutando en entorno: {ENTORNO}"'
    )
    
    procesar = BashOperator(
        task_id='procesar_datos',
        bash_command='echo "Procesando datos..."'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> verificar_entorno >> procesar >> fin
