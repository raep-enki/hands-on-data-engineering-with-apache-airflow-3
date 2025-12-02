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
y troubleshooting.

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

# Solución del challenge
def branch_by_amount(**context):
    amount = 3000  # Simulamos un monto
    if amount > 5000:
        return 'run_complex_ml_model'
    else:
        return 'run_simple_rules'

def make_decision(**context):
    score = 0.92  # Simulamos un score
    if score >= 0.95:
        return 'approve_transaction'
    elif score >= 0.80:
        return 'flag_for_review'
    else:
        return 'reject_transaction'

with DAG(
    dag_id='fraud_detection_realtime',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=['challenge', 'documentation'],
) as dag:
    
    dag.doc_md = __doc__
    
    start = EmptyOperator(task_id='start')
    start.doc_md = """# Start
Marca el inicio del pipeline de detección de fraude."""
    
    ingest_kafka_transactions = BashOperator(
        task_id='ingest_kafka_transactions',
        bash_command='echo "Consuming from Kafka topic: transactions"',
    )
    ingest_kafka_transactions.doc_md = """# Ingest Kafka Transactions

Consumo de transacciones desde Kafka.

**Configuración:**
- Topic: `transactions`
- Formato: JSON con campos `user_id`, `merchant_id`, `amount`, `timestamp`
- Consumer Group: `fraud-detection-group`

## Troubleshooting
- Si Kafka está caído: El consumer esperará hasta que se recupere (retry automático)
- Si mensaje malformado: Se loguea y se descarta
"""
    
    enrich_user_data = BashOperator(
        task_id='enrich_user_data',
        bash_command='echo "Enriching with user data from PostgreSQL"',
        execution_timeout=datetime.timedelta(seconds=30),
    )
    enrich_user_data.doc_md = """# Enrich User Data

Consulta PostgreSQL para obtener historial del usuario.

**Datos consultados:**
- Tabla: `users`
- Campos: `user_id`, `location`, `avg_transaction_amount`, `signup_date`, `last_transaction_date`
- Timeout: 30 segundos

## Troubleshooting
- Si timeout: Verificar conexión a Postgres y estado de índices
- Si usuario no existe: Se marca como transacción sospechosa
"""
    
    enrich_merchant_data = BashOperator(
        task_id='enrich_merchant_data',
        bash_command='echo "Enriching with merchant data from Redis"',
    )
    enrich_merchant_data.doc_md = """# Enrich Merchant Data

Consulta Redis para datos del comercio.

**Configuración:**
- Cache TTL: 5 minutos
- Campos: `merchant_id`, `category`, `rating`, `fraud_history`
- Fallback: Si Redis falla, consulta PostgreSQL directamente

## Por qué Redis
Usamos Redis por velocidad (< 10ms) ya que estos datos cambian poco.
"""
    
    calculate_fraud_features = BashOperator(
        task_id='calculate_fraud_features',
        bash_command='echo "Calculating 12 fraud features"',
    )
    calculate_fraud_features.doc_md = """# Calculate Fraud Features

Calcula 12 features para detección de fraude:

1. **Velocidad de transacciones**: Transacciones en última hora
2. **Distancia del hogar**: Km desde ubicación habitual
3. **Monto vs promedio**: Ratio con histórico del usuario
4. **Hora inusual**: Transacción fuera de horario habitual
5. **Merchant nuevo**: Primera vez con este comercio
6. **Categoría inusual**: Categoría no frecuente para el usuario
7. **País extranjero**: Transacción desde otro país
8. **Múltiples intentos**: Reintentos en corto tiempo
9. **Monto redondo**: Montos exactos (ej: $1000, $5000)
10. **Edad de cuenta**: Días desde signup
11. **Dispositivo nuevo**: Primera vez con este device_id
12. **IP sospechosa**: IP en blacklist

**Valores normales:**
- Velocidad: < 3 transacciones/hora
- Distancia: < 50 km
- Monto ratio: 0.5 - 2.0
"""
    
    branch_by_amount_task = BranchPythonOperator(
        task_id='branch_by_amount',
        python_callable=branch_by_amount,
    )
    branch_by_amount_task.doc_md = """# Branch By Amount

Decide qué modelo usar según el monto:
- Monto > $5000 → Modelo ML complejo
- Monto ≤ $5000 → Reglas simples

**Razón:** Montos altos justifican latencia adicional del modelo ML.
"""
    
    run_complex_ml_model = BashOperator(
        task_id='run_complex_ml_model',
        bash_command='echo "Running XGBoost model"',
    )
    run_complex_ml_model.doc_md = """# Run Complex ML Model

Modelo XGBoost entrenado con 100k muestras.

**Configuración:**
- Path: `/models/fraud_xgboost_v2.pkl`
- Versión: 2.3.1
- Accuracy: 94%
- Latencia máxima: 200ms
- Features: 12 calculados anteriormente

## Troubleshooting
- Si latencia > 200ms: Revisar carga del servidor
- Si modelo no carga: Usar fallback (reglas simples)
"""
    
    run_simple_rules = BashOperator(
        task_id='run_simple_rules',
        bash_command='echo "Running simple business rules"',
    )
    run_simple_rules.doc_md = """# Run Simple Rules

5 reglas de negocio simples:

1. Monto > $1000 Y velocidad > 5 transacciones/hora → Fraude
2. País extranjero Y edad cuenta < 7 días → Fraude
3. IP en blacklist → Fraude
4. Múltiples intentos fallidos (> 3) → Fraude
5. Distancia > 100 km Y tiempo < 1 hora desde última transacción → Fraude

**Por qué suficientes para montos bajos:**
Estas reglas capturan 85% de fraudes en transacciones < $5000.
"""
    
    make_decision_task = BranchPythonOperator(
        task_id='make_decision',
        python_callable=make_decision,
        trigger_rule='none_failed_min_one_success',
    )
    make_decision_task.doc_md = """# Make Decision

Decisión final según score de fraude:

- Score ≥ 0.95 → **Aprobar** (alta confianza)
- 0.80 ≤ Score < 0.95 → **Revisión manual** (zona gris)
- Score < 0.80 → **Rechazar** (alta probabilidad de fraude)

**SLA:** Esta decisión debe tomarse en < 500ms total.
"""
    
    approve_transaction = BashOperator(
        task_id='approve_transaction',
        bash_command='echo "Transaction approved"',
    )
    approve_transaction.doc_md = """# Approve Transaction

Aprueba la transacción y registra en audit log.

**Acciones:**
1. Envía aprobación al procesador de pagos
2. Registra en `audit_log` table
3. Actualiza métricas en tiempo real
"""
    
    flag_for_review = BashOperator(
        task_id='flag_for_review',
        bash_command='echo "Flagged for manual review"',
    )
    flag_for_review.doc_md = """# Flag For Review

Envía a cola de revisión manual.

**Proceso:**
1. Agrega a cola `pending_review` en Redis
2. Notifica a equipo de fraude (Slack)
3. Bloquea transacción temporalmente (24h max)

**SLA:** Revisión manual en < 2 horas
"""
    
    reject_transaction = BashOperator(
        task_id='reject_transaction',
        bash_command='echo "Transaction rejected"',
    )
    reject_transaction.doc_md = """# Reject Transaction

Rechaza la transacción y notifica al usuario.

**Acciones:**
1. Rechaza en procesador de pagos
2. Envía notificación al usuario (SMS/Email)
3. Registra en `rejected_transactions` table
4. Bloquea tarjeta temporalmente si múltiples rechazos
"""
    
    update_metrics = BashOperator(
        task_id='update_metrics',
        bash_command='echo "Updating Grafana dashboard"',
        trigger_rule='none_failed_min_one_success',
    )
    update_metrics.doc_md = """# Update Metrics

Actualiza dashboard de Grafana con métricas en tiempo real.

**Métricas:**
- Transacciones procesadas/hora
- Tasa de aprobación/rechazo
- Latencia promedio (P50, P95, P99)
- False positives/negatives
- Queue depth de revisión manual
"""
    
    end = EmptyOperator(task_id='end')
    end.doc_md = """# End
Finaliza el pipeline de detección de fraude."""
    
    # Dependencies
    start >> ingest_kafka_transactions
    ingest_kafka_transactions >> [enrich_user_data, enrich_merchant_data]
    [enrich_user_data, enrich_merchant_data] >> calculate_fraud_features
    calculate_fraud_features >> branch_by_amount_task
    branch_by_amount_task >> run_complex_ml_model >> make_decision_task
    branch_by_amount_task >> run_simple_rules >> make_decision_task
    make_decision_task >> approve_transaction >> update_metrics
    make_decision_task >> flag_for_review >> update_metrics
    make_decision_task >> reject_transaction >> update_metrics
    update_metrics >> end
