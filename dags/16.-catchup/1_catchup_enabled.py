"""
Catchup - Concepto Básico

Demuestra el comportamiento por defecto de catchup=True.
Cuando un DAG con catchup=True se activa, Airflow ejecuta
automáticamente todos los DAG runs desde start_date hasta ahora.

Este ejemplo tiene start_date en el pasado, por lo que al activarlo
generará múltiples DAG runs automáticamente.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='catchup_basic_enabled',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=True,  # Por defecto es True, ejecuta runs históricos
    tags=['example', 'catchup']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    process = BashOperator(
        task_id='process_daily_data',
        bash_command='echo "📅 Procesando datos para {{ ds }}"'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> process >> end

dag.doc_md = """
# Catchup Habilitado

Este DAG tiene `catchup=True` (comportamiento por defecto).

**Comportamiento:**
- Al activar el DAG por primera vez, Airflow creará DAG runs para cada día
  desde el `start_date` (2021-01-01) hasta la fecha actual
- Cada DAG run procesará el intervalo de datos correspondiente
- Los runs se ejecutarán secuencialmente o en paralelo según la configuración

**Cuándo usar catchup=True:**
- Cuando necesitas procesar datos históricos
- ETL que debe reconstruir un data warehouse desde cero
- Pipelines que deben procesar cada período de tiempo sin omitir ninguno

**Advertencia:**
- Con start_date muy antiguo, puede generar miles de DAG runs
- Asegúrate de que tu pipeline pueda manejar datos históricos
"""
