"""
Challenge: Pipelines con Deadlines del Mundo Real

Tu empresa tiene tres tipos de pipelines con diferentes criticidades de negocio. Cada uno tiene
un deadline estricto: si se pasa de tiempo, hay consecuencias (pérdida de dinero, incumplimiento
de SLAs, insatisfacción de clientes). Necesitas configurar alertas automáticas cuando se rompen.

Airflow 3.x permite definir `deadline` en cada tarea para alertar si se pasa del límite.

**DAG 1: Tier 1 - Procesamiento de Pagos (CRÍTICO)**

`deadline_challenge_tier1_payments` - Corre cada 15 minutos, deadline de 12 minutos.

Por qué es crítico: si los pagos tardan más de 12 min, los clientes ven errores de timeout
en el checkout, abandonan carritos, y la empresa pierde ventas directas ($$$).

**El flujo:**
`start` >> `ingest_pending_payments` (BashOperator - deadline 2 min, porque debe ser rápido) >>
`validate_payment_data` (BashOperator - deadline 1 min, validaciones simples) >>
`process_credit_cards` (BashOperator - deadline 5 min, integra con Stripe/PayPal) >>
`update_order_status` (BashOperator - deadline 2 min, marca pedidos como paid) >>
`send_confirmation_emails` (BashOperator - deadline 2 min, envía recibos) >> `end`

**Alertas:** Si alguna tarea pasa su deadline, envía PagerDuty alert (on_failure_callback)
porque requiere atención INMEDIATA.

**Config:**
- DAG ID: `deadline_challenge_tier1_payments`
- Schedule: `*/15 * * * *` (cada 15 minutos)
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'deadline_alerts', 'tier1_critical']`
- Total deadline del DAG: 12 min desde start

---

**DAG 2: Tier 2 - Reportes de Negocio (IMPORTANTE pero no crítico)**

`deadline_challenge_tier2_reporting` - Corre cada hora, deadline de 45 minutos.

Por qué es importante: los stakeholders esperan reportes actualizados cada hora para tomar
decisiones, pero pueden tolerar retrasos de unos minutos sin drama.

**El flujo:**
`start` >> `extract_sales_data` (BashOperator - deadline 10 min, query complejo en warehouse) >>
`extract_inventory_data` (BashOperator - deadline 10 min, otra query pesada) >>
`join_and_aggregate` (BashOperator - deadline 15 min, hace joins grandes) >>
`calculate_kpis` (BashOperator - deadline 5 min, fórmulas de negocio) >>
`generate_pdf_report` (BashOperator - deadline 3 min, renderiza gráficas) >>
`send_to_stakeholders` (BashOperator - deadline 2 min, envía email con PDF adjunto) >> `end`

**Alertas:** Si alguna tarea pasa su deadline, envía Slack alert (on_failure_callback)
para que el equipo investigue, pero no es emergencia de madrugada.

**Config:**
- DAG ID: `deadline_challenge_tier2_reporting`
- Schedule: `0 * * * *` (cada hora en punto)
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'deadline_alerts', 'tier2_important']`
- Total deadline del DAG: 45 min desde start

---

**DAG 3: Tier 3 - Batch Nocturno (RELAJADO, solo debe terminar antes del día siguiente)**

`deadline_challenge_tier3_batch` - Corre diario a las 01:00 AM, deadline de 20 horas.

Por qué es relajado: es un proceso batch que consolida TODO el día anterior. Tiene toda
la madrugada y mañana para completar. Solo importa que termine antes de las 9 PM del mismo día.

**El flujo:**
`start` >> `full_extract_all_sources` (BashOperator - deadline 5 hrs, extrae de 10 sistemas) >>
`data_quality_checks` (BashOperator - deadline 2 hrs, validaciones exhaustivas) >>
`transform_and_enrich` (BashOperator - deadline 8 hrs, transformaciones pesadas, ML features) >>
`load_to_data_lake` (BashOperator - deadline 3 hrs, escribe TBs a S3/Parquet) >>
`build_aggregated_tables` (BashOperator - deadline 2 hrs, crea tablas resumen para BI) >> `end`

**Alertas:** Si alguna tarea pasa su deadline, solo loguea warning (on_failure_callback simple)
porque hay tiempo de sobra para investigar en horario laboral.

**Config:**
- DAG ID: `deadline_challenge_tier3_batch`
- Schedule: `0 1 * * *` (diario a la 01:00 AM)
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'deadline_alerts', 'tier3_batch']`
- Total deadline del DAG: 20 horas desde start

---

**Callbacks sugeridos:**

Puedes simular callbacks con funciones Python simples:
```python
def alert_pagerduty(context):
    print(f"CRITICAL ALERT: {context['task_instance'].task_id} missed deadline!")

def alert_slack(context):
    print(f"WARNING: {context['task_instance'].task_id} missed deadline")

def log_warning(context):
    print(f"INFO: {context['task_instance'].task_id} took longer than expected")
```

Y asignarlos en las tareas:
- Tier 1: `on_failure_callback=alert_pagerduty`
- Tier 2: `on_failure_callback=alert_slack`
- Tier 3: `on_failure_callback=log_warning`

**Nota:** En Airflow 3.x el concepto de deadline está evolucionando. Si no está disponible,
usa `execution_timeout` como proxy para simular deadlines por tarea.

**Configuración técnica:**
- 3 DAGs separados en el mismo archivo
- Cada uno con schedule y deadlines diferentes
- Usa callbacks para alertas diferenciadas por tier
- Total: 15+ tareas entre los 3 DAGs
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Configura los 3 DAGs con deadlines y callbacks apropiados
