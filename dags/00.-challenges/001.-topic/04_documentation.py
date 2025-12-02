"""
Challenge: El Pipeline Que Nadie Entiende

Heredaste un sistema crítico de detección de fraude que procesa miles de transacciones por segundo.
El problema: el código no tiene documentación. Cuando algo falla, el equipo de soporte no sabe qué hacer.
Cuando llega alguien nuevo, tarda semanas en entender cómo funciona.

Tu misión: crear un pipeline tan bien documentado que cualquier persona nueva pueda entenderlo 
leyendo solo el DAG en la UI de Airflow. Debes usar `dag.doc_md` para explicar la arquitectura completa
y `task.doc_md` en cada tarea para explicar qué hace, por qué es importante, y qué pasa si falla.

**El flujo del pipeline de fraude:**

`start` (EmptyOperator) → `ingest_kafka_transactions` (BashOperator - consume topic de Kafka, 
debe documentar: qué topic, formato JSON esperado, qué hacer si Kafka está caído) →

`enrich_user_data` (BashOperator - consulta Postgres para datos del usuario: historial, ubicación,
debe documentar: qué tabla consulta, qué campos necesita, timeout de 30 seg) →

`enrich_merchant_data` (BashOperator - consulta Redis para datos del comercio: categoría, rating,
debe documentar: por qué usa Redis, cache de 5 min, fallback si Redis falla) →

Las dos tareas anteriores convergen en `calculate_fraud_features` (BashOperator - calcula 12 features:
velocidad de transacciones en última hora, distancia del hogar, monto vs promedio histórico, etc.
debe documentar: lista completa de features, fórmula de cada uno, valores normales esperados) →

`branch_by_amount` (BranchPythonOperator - decide path según monto de transacción):
- Si monto > $5000 → `run_complex_ml_model` (BashOperator - modelo XGBoost entrenado con 100k muestras,
  debe documentar: path del modelo, versión, accuracy esperado 94%, latencia máxima 200ms)
- Si monto <= $5000 → `run_simple_rules` (BashOperator - 5 reglas de negocio simples,
  debe documentar: listado de reglas, por qué son suficientes para montos bajos)

Ambos convergen en `make_decision` (BranchPythonOperator - decide acción final según score):
- Si score >= 0.95 → `approve_transaction` (BashOperator - aprueba y registra en audit log)
- Si 0.80 <= score < 0.95 → `flag_for_review` (BashOperator - envía a cola de revisión manual)
- Si score < 0.80 → `reject_transaction` (BashOperator - rechaza y notifica al usuario)

Todas convergen en `update_metrics` (BashOperator con `trigger_rule='none_failed_min_one_success'`
- actualiza dashboard de Grafana) → `end` (EmptyOperator).

**Documentación requerida:**

El `dag.doc_md` debe explicar: propósito del negocio, arquitectura (Kafka → Enrichment → ML → Decision),
SLAs esperados (99.9% uptime, <500ms latency), qué hacer en caso de incidente, enlaces a runbooks.

Cada tarea necesita `task.doc_md` con formato Markdown: título, descripción, inputs/outputs, configuración,
troubleshooting. Ejemplo: "# Enrich User Data\n\nConsulta PostgreSQL...\n\n## Troubleshooting\n- Si timeout..."

**Configuración técnica:**
- DAG ID: `fraud_detection_realtime`
- Schedule: @hourly
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'documentation']`
- Usa `dag.doc_md = __doc__` para reutilizar el docstring del módulo
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Crea el pipeline con documentación rica en markdown
