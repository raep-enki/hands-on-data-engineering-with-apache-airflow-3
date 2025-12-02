"""
Catchup - Uso de Logical Date

Demuestra cómo usar logical_date en tareas para procesar
el intervalo de datos correcto cuando catchup=True.

El logical_date representa el inicio del intervalo de datos
que el DAG run debe procesar.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='catchup_with_logical_date',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=True,
    tags=['example', 'core_concepts', 'dag_runs', 'catchup']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Usando macros de Airflow para acceder a logical_date
    extract = BashOperator(
        task_id='extract_for_date',
        bash_command="""
        echo "📅 Logical Date: {{ logical_date }}"
        echo "📅 DS (date string): {{ ds }}"
        echo "📊 Data Interval Start: {{ data_interval_start }}"
        echo "📊 Data Interval End: {{ data_interval_end }}"
        echo "⏰ Execution Date (deprecated): usa logical_date"
        """
    )
    
    # Simulando extracción de datos para la fecha específica
    process = BashOperator(
        task_id='process_specific_date',
        bash_command='echo "⚙️ Procesando datos del día {{ ds }}"'
    )
    
    # Guardando con timestamp del intervalo
    save = BashOperator(
        task_id='save_with_timestamp',
        bash_command='echo "💾 Guardando: output_{{ ds_nodash }}.csv"'
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> extract >> process >> save >> end

dag.doc_md = """
# Catchup y Logical Date

Este DAG demuestra el uso correcto de `logical_date` con catchup.

**Variables de contexto importantes:**
- `{{ logical_date }}`: Fecha lógica del DAG run (inicio del intervalo)
- `{{ ds }}`: Date string en formato YYYY-MM-DD
- `{{ ds_nodash }}`: Date string sin guiones (YYYYMMDD)
- `{{ data_interval_start }}`: Inicio del intervalo de datos
- `{{ data_interval_end }}`: Fin del intervalo de datos

**Airflow 3.x - Cambios importantes:**
- `execution_date` está deprecated → usar `logical_date`
- `logical_date` representa el inicio del período de datos
- Para DAG diario con schedule='@daily':
  - logical_date = 2021-01-01 00:00:00
  - data_interval_start = 2021-01-01 00:00:00
  - data_interval_end = 2021-01-02 00:00:00

**Ejemplo práctico:**
Si el DAG procesa datos diarios y tiene logical_date=2021-01-15,
debe procesar todos los datos del día 2021-01-15 (desde 00:00 hasta 23:59:59).

**Uso en queries SQL:**
```sql
SELECT * FROM events 
WHERE event_date = '{{ ds }}'
```

**Uso en nombres de archivo:**
```bash
output_{{ ds_nodash }}.parquet  # output_20210115.parquet
```
"""
