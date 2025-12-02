"""
Challenge: Dashboard Tiempo Real vs Analytics Pesados

Tienes un dashboard que se actualiza cada hora con métricas en tiempo real. Es crítico que esté
fresco. Pero también calculas analytics pesados (trends históricos, agregados complejos, rebuild
de tablas) que tardan mucho y consumen recursos.

El problema: si el scheduler se cayó 5 horas, al volver va a ejecutar 5 runs atrasados (catchup).
NO tiene sentido recalcular trends históricos y rebuilds para las 4 ejecuciones viejas, solo para
la más reciente. Pero SÍ quieres actualizar el dashboard con los datos de cada hora perdida.

La solución: `LatestOnlyOperator` separa tareas que solo corren en el run más nuevo de las que
siempre corren. Es un pattern de optimización de backfills.

**El flujo con LatestOnly:**

`start` (EmptyOperator) se ramifica en 2 paths paralelos:

**Path 1: Tareas que SIEMPRE corren (cada hora perdida):**
`start` >> `ingest_hourly_data` (BashOperator - extrae transacciones de la última hora desde Kafka,
USA {{ logical_date }} para saber qué hora procesar. Si hubo 5 horas perdidas, corre 5 veces) >>

`validate_hourly_data` (BashOperator - verifica formato, missing values. Cada hora es independiente) >>

`calculate_hourly_metrics` (BashOperator - cuenta transacciones, suma montos, calcula promedios
de esta hora específica. Usa {{ ds }}, {{ ts }} para identificar datos) >>

`update_realtime_dashboard` (BashOperator - inserta métricas en tabla `dashboard_current_hour`
que actualiza la UI en tiempo real. Cada hora debe actualizar su slot correspondiente) >> `end`

**Path 2: Tareas que SOLO corren en el run más reciente (LatestOnly):**
`start` >> `check_latest_only` (LatestOnlyOperator - evalúa si este run es el más nuevo.
Si es viejo (backfill), skippea todo downstream. Si es el más reciente, continúa normal) >>

`calculate_7day_trends` (BashOperator - recalcula promedios móviles de últimos 7 días,
identifica tendencias alcistas/bajistas. SOLO corre para el run más nuevo porque si tienes
5 horas atrasadas, no necesitas recalcular tendencias 5 veces, solo 1 vez con los datos más frescos) >>

`calculate_30day_aggregations` (BashOperator - suma totales mensuales, identifica patrones
semanales, calcula percentiles. Proceso pesado que tarda 10+ min. SOLO corre en latest) >>

`rebuild_analytics_tables` (BashOperator - hace VACUUM, ANALYZE, reconstruye índices de tablas
de analytics. Proceso MUY pesado (30+ min) que solo necesitas hacer una vez con datos actuales) >>

`send_executive_report` (BashOperator - envía reporte semanal a ejecutivos con insights.
SOLO en latest porque no tiene sentido enviar 5 reportes si hubo 5 horas perdidas) >> `end`

**Comportamiento en diferentes escenarios:**

1. **Run normal (no hay backfill):**
   - `check_latest_only` detecta que ES el latest → todas las tareas corren
   - Path 1: ingest_hourly_data + métricas → actualiza dashboard
   - Path 2: trends + agregados + rebuild → completan normalmente

2. **Backfill de 5 horas atrasadas:**
   - Runs viejos (horas 1-4): 
     * Path 1 corre normal (recupera datos de cada hora perdida)
     * Path 2: `check_latest_only` detecta que NO es latest → SKIPPEA todo downstream
   - Run más nuevo (hora 5):
     * Path 1 corre normal
     * Path 2 corre COMPLETO (trends, agregados, rebuild) solo 1 vez

**Por qué es útil:**
- Optimización: evita procesar 5 veces lo mismo que solo necesitas 1 vez
- Costos: rebuild de tablas consume CPU/memoria, solo hazlo cuando importa
- Negocio: reportes ejecutivos solo envías 1 vez, no spam de 5 emails

**Configuración técnica:**
- DAG ID: `real_time_dashboard_update`
- Schedule: `0 * * * *` (cada hora en punto)
- Start date: 2024-01-01
- Catchup: True (para demostrar el pattern cuando hay backfill)
- Tags: `['challenge', 'latest_only']`
- Usa `LatestOnlyOperator` como gate para tareas pesadas
- Total: ~10 tareas con 2 paths claros
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

# TODO: Implementa el pattern latest-only para optimizar backfills
