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

# TODO: Crea ambos pipelines con estrategias opuestas de catchup
