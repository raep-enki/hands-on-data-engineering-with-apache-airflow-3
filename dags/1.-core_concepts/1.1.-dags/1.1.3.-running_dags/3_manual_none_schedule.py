"""
Schedule None - Trigger Manual

Demuestra un DAG sin schedule automático que solo se ejecuta por trigger manual.
Útil para procesos bajo demanda que requieren intervención o aprobación humana.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id='despliegue_manual_produccion',
    start_date=datetime.datetime(2024, 1, 1),
    schedule=None,  # Sin schedule automático
    catchup=False,
    description='Despliegue a producción - Solo trigger manual',
    tags=['example', 'core_concepts', 'dags', 'running_dags']
):
    inicio = EmptyOperator(task_id='inicio')
    
    validar_codigo = BashOperator(
        task_id='validar_codigo',
        bash_command='echo "Validando código antes del despliegue"'
    )
    
    ejecutar_pruebas = BashOperator(
        task_id='ejecutar_pruebas',
        bash_command='echo "Ejecutando suite de pruebas"'
    )
    
    crear_backup = BashOperator(
        task_id='crear_backup',
        bash_command='echo "Creando backup antes del despliegue"'
    )
    
    desplegar = BashOperator(
        task_id='desplegar_aplicacion',
        bash_command='echo "Desplegando aplicación a producción"'
    )
    
    verificar = BashOperator(
        task_id='verificar_despliegue',
        bash_command='echo "Verificando que el despliegue fue exitoso"'
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> validar_codigo >> ejecutar_pruebas >> crear_backup
    crear_backup >> desplegar >> verificar >> fin
