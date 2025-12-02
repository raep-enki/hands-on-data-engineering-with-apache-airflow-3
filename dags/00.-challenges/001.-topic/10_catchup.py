"""
Challenge: Dos Estrategias de Carga Completamente Diferentes

Tu empresa tiene dos sistemas con filosofías opuestas de carga de datos. Debes crear ambos
DAGs para demostrar que entiendes las diferencias entre full refresh e incremental, y cuándo
usar `catchup=False` vs `catchup=True`.

**DAG 1: Full Refresh (catchup=False)**

`catchup_challenge_full_refresh` - Ejecuta cada lunes a las 00:00, start_date hace 30 días.

El propósito: refresca toda la tabla de productos del e-commerce desde cero cada semana.
No importa qué pasó en semanas anteriores, solo importa el estado actual.

**Por qué catchup=False:**
Si activas este DAG hoy (y tiene start_date hace 30 días), solo debe correr 1 vez (el presente).
NO debe correr 4 veces para "recuperar" las 4 semanas pasadas, porque no hay nada que recuperar:
es full refresh, borra todo y recarga todo cada vez. El pasado no existe para este DAG.

**El flujo (full refresh):**
`start` >> `truncate_products_table` (BashOperator - borra TODA la tabla products) >>
`extract_from_source_system` (BashOperator - extrae estado actual completo desde Oracle) >>
`validate_completeness` (BashOperator - verifica que trajo 100% de registros esperados) >>
`load_to_warehouse` (BashOperator - inserta bulk de 1 millón de productos) >>
`rebuild_indexes` (BashOperator - recrea índices y estadísticas) >>
`notify_refresh_complete` (BashOperator - notifica a equipo de analytics) >> `end`

**Configuración DAG 1:**
- DAG ID: `catchup_challenge_full_refresh`
- Schedule: `0 0 * * 1` (cada lunes a las 00:00)
- Start date: 30 días atrás desde hoy
- **Catchup: False** (crítico! solo corre el presente)
- Tags: `['challenge', 'catchup', 'full_refresh']`

---

**DAG 2: Incremental (catchup=True)**

`catchup_challenge_incremental` - Ejecuta cada hora, start_date hace 7 días.

El propósito: procesa transacciones hora por hora. Cada hora es un batch independiente.
Si el sistema estuvo apagado, necesitas "recuperar" todas las horas perdidas.

**Por qué catchup=True:**
Si activas este DAG hoy (y tiene start_date hace 7 días), debe correr 168 veces (7 días × 24 horas)
para recuperar TODAS las horas faltantes. Cada hora tiene transacciones únicas que no puedes
perder. Es incremental: procesa solo lo nuevo de cada hora.

**El flujo (incremental):**
`start` >> `identify_hour_to_process` (BashOperator - usa {{ logical_date }} para saber qué hora procesar) >>
`extract_transactions_for_hour` (BashOperator - extrae solo transacciones de esa hora específica) >>
`filter_duplicates` (BashOperator - verifica que no existan ya en warehouse con mismo timestamp) >>
`calculate_hourly_aggregations` (BashOperator - suma ventas, cuenta transacciones de esa hora) >>
`append_to_warehouse` (BashOperator - INSERT incremental, no DELETE) >>
`update_watermark` (BashOperator - marca que esta hora ya fue procesada) >>
`send_hourly_report` (BashOperator - envía métrica de esa hora a monitoring) >> `end`

**Configuración DAG 2:**
- DAG ID: `catchup_challenge_incremental`
- Schedule: `0 * * * *` (cada hora en punto: 00:00, 01:00, 02:00...)
- Start date: 7 días atrás desde hoy
- **Catchup: True** (crítico! recupera todas las horas faltantes)
- **max_active_runs: 3** (procesa máximo 3 horas en paralelo para no saturar)
- Tags: `['challenge', 'catchup', 'incremental']`

---

**Diferencias clave (debes demostrar que entiendes):**

| Aspecto | Full Refresh (catchup=False) | Incremental (catchup=True) |
|---------|------------------------------|----------------------------|
| Filosofía | Reemplaza todo cada vez | Agrega solo lo nuevo |
| Historia | No le importa el pasado | Recupera cada período faltante |
| Operación | TRUNCATE + INSERT | INSERT incremental |
| Si estuvo apagado | Solo corre 1 vez (ahora) | Corre N veces (recupera todo) |
| Idempotencia | No necesita (borra todo) | Crítica (filter_duplicates) |
| Uso de logical_date | No importa la fecha | Crítico para saber qué procesar |

**Configuración técnica general:**
- Ambos DAGs en el mismo archivo
- Usa `datetime.datetime.now() - datetime.timedelta(days=N)` para calcular start_date
- Full refresh: schedule semanal, start_date 30 días atrás
- Incremental: schedule horario, start_date 7 días atrás, max_active_runs=3
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Solución del challenge - DAG 1: Full Refresh (catchup=False)
with DAG(
    dag_id='catchup_challenge_full_refresh',
    start_date=datetime.datetime.now() - datetime.timedelta(days=30),
    schedule='0 0 * * 1',  # Cada lunes a las 00:00
    catchup=False,
    tags=['challenge', 'catchup', 'full_refresh'],
) as dag1:
    
    start = EmptyOperator(task_id='start')
    
    truncate_products_table = BashOperator(
        task_id='truncate_products_table',
        bash_command='echo "TRUNCATE TABLE products"',
    )
    
    extract_from_source_system = BashOperator(
        task_id='extract_from_source_system',
        bash_command='echo "Extracting all products from Oracle"',
    )
    
    validate_completeness = BashOperator(
        task_id='validate_completeness',
        bash_command='echo "Validating 100% of records extracted"',
    )
    
    load_to_warehouse = BashOperator(
        task_id='load_to_warehouse',
        bash_command='echo "Bulk loading 1M products"',
    )
    
    rebuild_indexes = BashOperator(
        task_id='rebuild_indexes',
        bash_command='echo "Rebuilding indexes and statistics"',
    )
    
    notify_refresh_complete = BashOperator(
        task_id='notify_refresh_complete',
        bash_command='echo "Notifying analytics team"',
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> truncate_products_table >> extract_from_source_system >> validate_completeness
    validate_completeness >> load_to_warehouse >> rebuild_indexes >> notify_refresh_complete >> end

# DAG 2: Incremental (catchup=True)
with DAG(
    dag_id='catchup_challenge_incremental',
    start_date=datetime.datetime.now() - datetime.timedelta(days=7),
    schedule='0 * * * *',  # Cada hora en punto
    catchup=True,
    max_active_runs=3,
    tags=['challenge', 'catchup', 'incremental'],
) as dag2:
    
    start = EmptyOperator(task_id='start')
    
    identify_hour_to_process = BashOperator(
        task_id='identify_hour_to_process',
        bash_command='echo "Processing hour: {{ logical_date }}"',
    )
    
    extract_transactions_for_hour = BashOperator(
        task_id='extract_transactions_for_hour',
        bash_command='echo "Extracting transactions for {{ ds }} {{ logical_date.hour }}:00"',
    )
    
    filter_duplicates = BashOperator(
        task_id='filter_duplicates',
        bash_command='echo "Filtering duplicates with timestamp {{ logical_date }}"',
    )
    
    calculate_hourly_aggregations = BashOperator(
        task_id='calculate_hourly_aggregations',
        bash_command='echo "Calculating hourly aggregations for {{ ds }}"',
    )
    
    append_to_warehouse = BashOperator(
        task_id='append_to_warehouse',
        bash_command='echo "INSERT incremental for hour {{ logical_date }}"',
    )
    
    update_watermark = BashOperator(
        task_id='update_watermark',
        bash_command='echo "Marking hour {{ logical_date }} as processed"',
    )
    
    send_hourly_report = BashOperator(
        task_id='send_hourly_report',
        bash_command='echo "Sending metrics for hour {{ logical_date }}"',
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> identify_hour_to_process >> extract_transactions_for_hour >> filter_duplicates
    filter_duplicates >> calculate_hourly_aggregations >> append_to_warehouse
    append_to_warehouse >> update_watermark >> send_hourly_report >> end
