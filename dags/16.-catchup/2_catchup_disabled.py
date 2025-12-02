"""
Catchup - Deshabilitado

Demuestra el uso de catchup=False.
Con catchup=False, Airflow solo ejecuta el DAG run más reciente,
ignorando todos los intervalos históricos.

Este es el comportamiento recomendado para la mayoría de los DAGs nuevos.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='catchup_disabled',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,  # No ejecuta runs históricos
    tags=['example', 'catchup']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    process = BashOperator(
        task_id='process_latest_data',
        bash_command='echo "📅 Procesando solo datos más recientes: {{ ds }}"'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> process >> end

dag.doc_md = """
# Catchup Deshabilitado

Este DAG tiene `catchup=False`.

**Comportamiento:**
- Al activar el DAG, solo se crea el DAG run más reciente
- No se ejecutan intervalos históricos automáticamente
- Comienza a ejecutarse desde el siguiente intervalo programado

**Cuándo usar catchup=False:**
- DAGs que solo necesitan procesar datos actuales
- Pipelines de monitoreo en tiempo real
- Procesos que no tienen sentido ejecutar históricamente
- DAGs nuevos que no necesitan reprocessar el pasado
- ML pipelines que solo entrenan con datos recientes

**Ejemplo de uso:**
- Reportes diarios que solo importan hoy
- Sincronización de APIs en tiempo real
- Limpieza de archivos temporales
"""
