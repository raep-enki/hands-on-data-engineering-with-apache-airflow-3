"""
Catchup - Start Date Reciente

Demuestra cómo usar catchup=True con un start_date reciente
para controlar el número de runs generados.

Al usar un start_date cercano a la fecha actual, se limita
la cantidad de DAG runs históricos que se crearán.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum

with DAG(
    dag_id='catchup_recent_start_date',
    schedule='@hourly',
    start_date=pendulum.now().subtract(days=2),  # Solo últimos 2 días
    catchup=True,
    tags=['example', 'core_concepts', 'dag_runs', 'catchup']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract = BashOperator(
        task_id='extract_hourly',
        bash_command='echo "📥 Extrayendo datos horarios: {{ ts }}"'
    )
    
    process = BashOperator(
        task_id='process_hourly',
        bash_command='echo "⚙️ Procesando hora: {{ data_interval_start }} a {{ data_interval_end }}"'
    )
    
    load = BashOperator(
        task_id='load_hourly',
        bash_command='echo "📤 Cargando datos procesados"'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> extract >> process >> load >> end

dag.doc_md = """
# Catchup con Start Date Reciente

Este DAG usa `catchup=True` pero con `start_date` reciente.

**Configuración:**
- schedule: `@hourly` (cada hora)
- start_date: 2 días atrás desde ahora
- catchup: `True`

**Comportamiento:**
- Al activar, generará ~48 DAG runs (2 días × 24 horas)
- Cantidad manejable de runs históricos
- Útil para "catch up" corto sin abrumar el scheduler

**Estrategia recomendada:**
1. Para DAGs nuevos que necesitan procesar últimos días/semanas
2. Usar start_date dinámico: `pendulum.now().subtract(days=N)`
3. Controlar el volumen de runs históricos
4. Evitar sobrecarga del scheduler

**Cálculo de runs:**
- Diario por 30 días = 30 runs
- Horario por 7 días = 168 runs
- Cada 5 min por 1 día = 288 runs
"""
