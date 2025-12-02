"""
Challenge: Data Pipeline Completo con Asset Definitions

Objetivo:
=========
Crear un data pipeline completo con 4 DAGs usando Asset Definitions
para orquestar el flujo basado en disponibilidad de datos.

Requisitos:
===========

1. Assets (4 assets):
   - raw_events: 's3://bucket/raw/events.json'
   - processed_events: 's3://bucket/processed/events.parquet'
   - user_profiles: 's3://bucket/processed/users.parquet'
   - analytics_report: 's3://bucket/reports/analytics.json'

2. DAG 1: 'extract_events'
   - Schedule: '@hourly'
   - Catchup: False
   - Tags: ['challenge', 'authoring_and_scheduling', 'asset_definitions', 'producer']
   - Task: extract_raw_events (@task)
     * outlets: [raw_events]
     * Simula extracción de 1000 eventos
     * Print: "Extracting raw events..."
     * Return: {'count': 1000, 'timestamp': logical_date}

3. DAG 2: 'process_events'
   - Schedule: [raw_events]  (triggered por DAG 1)
   - Catchup: False
   - Tags: ['challenge', 'authoring_and_scheduling', 'asset_definitions', 'transform']
   - Tasks:
     A. clean_events (@task)
        * outlets: [processed_events]
        * Limpia eventos (simula remover 5% inválidos)
        * Return: {'valid_count': 950}
     
     B. extract_users (@task)
        * outlets: [user_profiles]
        * Extrae unique users de eventos
        * Return: {'users_count': 200}
   
   - Dependencia: ambas tasks en paralelo

4. DAG 3: 'generate_analytics'
   - Schedule: [processed_events, user_profiles]  (espera ambos)
   - Catchup: False
   - Tags: ['challenge', 'authoring_and_scheduling', 'asset_definitions', 'analytics']
   - Tasks:
     A. calculate_metrics (@task)
        * Calcula métricas:
          - events_per_user = processed_events / users
          - engagement_rate = 0.75 (simulado)
        * Return: dict con metrics
     
     B. generate_report (@task)
        * outlets: [analytics_report]
        * Recibe metrics de calculate_metrics
        * Genera reporte JSON
        * Return: {'status': 'complete', 'metrics': metrics}
   
   - Dependencia: calculate_metrics >> generate_report

5. DAG 4: 'publish_dashboard'
   - Schedule: [analytics_report]  (triggered por DAG 3)
   - Catchup: False
   - Tags: ['challenge', 'authoring_and_scheduling', 'asset_definitions', 'consumer']
   - Task: publish_to_dashboard (@task)
     * Lee analytics_report
     * Print: "Publishing to dashboard..."
     * Print metrics
     * Return: {'dashboard_url': 'https://dashboard.example.com'}

6. Asset Metadata:
   - Cada asset debe incluir extra con:
     * format: 'json' o 'parquet'
     * owner: nombre del equipo
     * description: breve descripción

Pipeline Flow:
==============
```
extract_events (@hourly)
  ↓ produces raw_events
process_events (triggered)
  ↓ produces processed_events + user_profiles
generate_analytics (triggered when both ready)
  ↓ produces analytics_report
publish_dashboard (triggered)
```

Tips:
=====
- Asset definition: Asset('uri', extra={'key': 'value'})
- Producer: @task(outlets=[asset])
- Consumer: DAG(schedule=[asset1, asset2])
- Multiple outlets: @task(outlets=[asset1, asset2])
- Task dependency: task1 >> task2

Evaluación:
===========
- ✅ 4 DAGs creados correctamente
- ✅ Assets definidos con metadata
- ✅ Producers usan outlets parameter
- ✅ Consumers usan schedule=[assets]
- ✅ Flow completo funciona (lineage visible)
- ✅ DAG 3 espera ambos assets antes de ejecutar

Output esperado (secuencia):
=============================
```
[DAG 1 - hourly run]
Extracting raw events...
✅ Asset updated: s3://bucket/raw/events.json

[DAG 2 - triggered by raw_events]
Cleaning events...
✅ Asset updated: s3://bucket/processed/events.parquet
Extracting users...
✅ Asset updated: s3://bucket/processed/users.parquet

[DAG 3 - triggered by both processed_events and user_profiles]
Calculating metrics...
  - Events per user: 4.75
  - Engagement rate: 0.75
Generating report...
✅ Asset updated: s3://bucket/reports/analytics.json

[DAG 4 - triggered by analytics_report]
Publishing to dashboard...
✅ Dashboard URL: https://dashboard.example.com
```

Restricciones:
==============
- NO usar schedule='@daily' excepto en DAG 1
- Todos los otros DAGs usan schedule=[assets]
- NO usar ExternalTaskSensor
- Usar @task decorator (TaskFlow API)
- Cada DAG debe ser independiente (no compartir código entre DAGs)
"""

# TU CÓDIGO AQUÍ
# Necesitas crear 4 DAGs en este archivo
# Puedes usar múltiples bloques with DAG(...):
