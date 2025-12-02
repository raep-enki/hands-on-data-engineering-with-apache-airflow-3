"""
Challenge: Tareas Que Se Quedan Colgadas Para Siempre

Tienes un pipeline de monitoreo que a veces se queda trabado: una tarea que consulta una API externa
lenta se queda esperando indefinidamente (API de terceros sin SLA). Otra tarea que procesa logs pesados
nunca termina (archivos de 10GB+). El pipeline entero se detiene y nadie sabe por qué hasta que
revisas manualmente 3 horas después.

Necesitas timeouts inteligentes y SLAs (Service Level Agreements) diferenciados por tipo de tarea.

**El flujo con timeouts estratégicos:**

`start` (EmptyOperator) >>

`ping_external_service` (BashOperator - hace ping a API de terceros, debe responder en 10 seg o fallar.
Timeout: `execution_timeout=timedelta(seconds=10)`. Retries: 5 porque la API es inestable.
Retry_delay: 30 seg. Si falla después de 5 intentos, el pipeline continúa igual) >>

`fetch_api_data` (BashOperator - consulta API REST para traer datos, debe completar en 2 min.
Timeout: `execution_timeout=timedelta(minutes=2)`. Retries: 3. SLA: 5 minutos desde start del DAG.
Por qué: negocio requiere datos frescos cada 30 min, si tarda más de 5 min el reporte llega tarde) >>

`validate_api_response` (BashOperator - valida JSON schema, debe ser instantáneo, timeout 30 seg.
Timeout: `execution_timeout=timedelta(seconds=30)`. Retries: 1 (si falla es por bug, no por timeout).
SLA: 6 minutos. Por qué: validación es crítica para detectar cambios en API upstream) >>

Se ramifica en 3 procesamientos paralelos con diferentes características:

**Procesamiento urgente (timeout corto, SLA estricto):**
`process_realtime_alerts` (BashOperator - detecta anomalías críticas que requieren acción inmediata.
Timeout: `execution_timeout=timedelta(minutes=3)`. Retries: 2.
SLA: 10 minutos. Por qué: alertas críticas deben dispararse rápido o se pierde el incidente) >>

**Procesamiento estándar (timeout medio):**
`process_business_metrics` (BashOperator - calcula KPIs para dashboards, puede tardar un poco.
Timeout: `execution_timeout=timedelta(minutes=10)`. Retries: 2.
SLA: 20 minutos. Por qué: dashboards se actualizan cada hora, no es urgente pero debe completar) >>

**Procesamiento pesado (timeout largo, sin SLA estricto):**
`process_historical_logs` (BashOperator - analiza 10GB de logs para tendencias, puede tardar mucho.
Timeout: `execution_timeout=timedelta(hours=1)`. Retries: 1 porque si falla es por recursos.
SLA: None. Por qué: es análisis batch, no time-sensitive, puede tardar lo que necesite) >>

**Agregación y validación final (SLA crítico):**
`[process_realtime_alerts, process_business_metrics, process_historical_logs]` >>
`aggregate_all_results` (BashOperator - consolida resultados de los 3 procesamientos.
Timeout: `execution_timeout=timedelta(minutes=5)`. Retries: 3.
SLA: 25 minutos. Por qué: el reporte final debe estar listo antes del próximo ciclo de 30 min) >>

`send_to_dashboard` (BashOperator - actualiza Grafana dashboard via API.
Timeout: `execution_timeout=timedelta(minutes=2)`. Retries: 5 porque Grafana a veces está saturado.
SLA: 28 minutos. Por qué: debe completar antes del siguiente run para no acumular retrasos) >>

`cleanup` (BashOperator con `trigger_rule='all_done'` - limpia archivos temporales SIEMPRE.
Timeout: `execution_timeout=timedelta(minutes=1)`. Retries: 0 porque limpieza no es crítica) >>

`end` (EmptyOperator)

**Configuración de timeouts por tipo de tarea:**
- APIs externas: timeout corto (10 seg - 2 min), retries altos (3-5), porque son rápidas pero inestables
- Validaciones: timeout muy corto (30 seg), retries mínimos (1), porque deben ser instantáneas
- Procesamiento ligero: timeout medio (3-10 min), retries normales (2)
- Procesamiento pesado: timeout largo (1 hora), retries bajos (1), porque fallas son por recursos

**Configuración de SLAs por criticidad:**
- Crítico: SLA 6-10 min (validación, alertas) - negocio requiere respuesta rápida
- Estándar: SLA 20-25 min (métricas, agregación) - debe completar antes del próximo ciclo
- Batch: SLA None (logs históricos) - no time-sensitive

**Configuración técnica:**
- DAG ID: `timeout_challenge`
- Schedule: `*/30 * * * *` (cada 30 minutos)
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'timeouts']`
- Usa `from datetime import timedelta` para los timeouts
- Total: 10 tareas con timeouts y SLAs diferenciados
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Define el pipeline con timeouts y SLAs estratégicos
