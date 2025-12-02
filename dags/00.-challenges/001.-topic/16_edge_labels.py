"""
Challenge: El Pipeline Con Decisiones Que Nadie Entiende

Miras el grafo de un pipeline complejo y ves: una tarea se conecta a tres tareas diferentes. ¿Por qué?
¿Qué significa cada camino? Otra tarea tiene dos salidas: ¿cuándo va por una y cuándo por otra?
El grafo no te dice nada, solo muestra flechas sin contexto.

Tu misión: agregar **edge labels** (etiquetas en las conexiones) explicando qué significan.
"Esta flecha es cuando la calidad es > 95%". "Esta otra es cuando la validación falla y necesita
reprocesar". Son como comentarios inline, pero en el grafo visual.

Airflow permite usar `Label()` para documentar decisiones en las flechas del grafo.

**El flujo con decisiones documentadas:**

`start` (EmptyOperator) >> `ingest_customer_data` (BashOperator - carga datos raw) >>

`validate_data_schema` (BashOperator - verifica schema correcto) >>

**Primera decisión con branches documentados:**

`branch_by_data_quality` (BranchPythonOperator - evalúa quality score):
- Si score >= 95 >> Label("Quality Excellent (>=95)") >> `process_premium_path` 
  (BashOperator - procesamiento completo con analytics avanzados)
- Si 80 <= score < 95 >> Label("Quality Good (80-95)") >> `process_standard_path`
  (BashOperator - procesamiento estándar sin features extra)
- Si score < 80 >> Label("Quality Poor (<80)") >> `flag_for_manual_review`
  (BashOperator - envía a cola de revisión humana)

Los 3 paths convergen en `run_base_transformations` (BashOperator con `trigger_rule='none_failed_min_one_success'`
aplica transformaciones comunes a todos los paths) >>

**Segunda decisión con branches documentados:**

`branch_by_record_count` (BranchPythonOperator - evalúa volumen de datos):
- Si count >= 100k >> Label("High Volume (100k+ records)") >> `use_spark_processing`
  (BashOperator - usa Spark para procesar millones de registros eficientemente)
- Si 10k <= count < 100k >> Label("Medium Volume (10k-100k)") >> `use_pandas_processing`
  (BashOperator - usa Pandas, suficiente para decenas de miles)
- Si count < 10k >> Label("Low Volume (<10k)") >> `use_simple_python`
  (BashOperator - usa Python vanilla, no necesita librerías pesadas)

Los 3 paths convergen en `validate_processing_results` (BashOperator con `trigger_rule='none_failed_min_one_success'`
verifica que el output es correcto independiente del path) >>

**Tercera decisión con branches documentados:**

`check_business_metrics` (BashOperator - calcula métricas de negocio) >>

`branch_by_metrics_health` (BranchPythonOperator - evalúa si métricas están OK):
- Si métricas OK >> Label("Metrics Healthy") >> `load_to_production`
  (BashOperator - carga directo a producción)
- Si métricas degradadas >> Label("Metrics Degraded - Investigation Required") >> `create_incident_ticket`
  (BashOperator - abre ticket en Jira para investigación)

Ambos convergen en `update_monitoring_dashboard` (BashOperator con `trigger_rule='none_failed_min_one_success'`
actualiza Grafana con resultados) >>

**Paths finales paralelos con labels:**

De `update_monitoring_dashboard` se ramifican 3 tareas finales:
- Label("Send to Data Lake") >> `archive_to_s3` (BashOperator - backup en S3)
- Label("Update Cache") >> `refresh_redis_cache` (BashOperator - invalida cache viejo)
- Label("Notify Stakeholders") >> `send_email_report` (BashOperator - envía resumen)

Todas convergen en `end` (EmptyOperator)

**Cómo usar Label():**

```python
from airflow.utils.edgemodifier import Label

# Opción 1: Inline con >>
branch_task >> Label("When quality >= 95") >> high_quality_task

# Opción 2: Con listas
[task1, task2] >> Label("Both completed successfully") >> next_task

# Opción 3: Múltiples labels
branch >> Label("Path A: Premium") >> taskA
branch >> Label("Path B: Standard") >> taskB
branch >> Label("Path C: Review") >> taskC
```

**Beneficios de edge labels:**
- **Documentación visual:** El grafo explica por sí solo las decisiones
- **Onboarding:** Nuevos miembros entienden la lógica sin leer código
- **Debugging:** Rápido ver qué path tomó un run y por qué
- **Comunicación:** Puedes compartir el grafo con stakeholders no técnicos

**Configuración técnica:**
- DAG ID: `conditional_data_pipeline`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'edge_labels', 'documentation']`
- Usa `Label()` en al menos 10 conexiones diferentes
- 3 BranchPythonOperators con múltiples paths documentados
- Total: 15+ tareas con decisiones explicadas visualmente
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

# Solución del challenge

def branch_by_data_quality(**context):
    score = 88  # Simulación
    if score >= 95:
        return 'process_premium_path'
    elif score >= 80:
        return 'process_standard_path'
    else:
        return 'flag_for_manual_review'

def branch_by_record_count(**context):
    count = 50000  # Simulación
    if count >= 100000:
        return 'use_spark_processing'
    elif count >= 10000:
        return 'use_pandas_processing'
    else:
        return 'use_simple_python'

def branch_by_metrics_health(**context):
    metrics_ok = True  # Simulación
    if metrics_ok:
        return 'load_to_production'
    else:
        return 'create_incident_ticket'

with DAG(
    dag_id='conditional_data_pipeline',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['challenge', 'edge_labels', 'documentation'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    ingest_customer_data = BashOperator(
        task_id='ingest_customer_data',
        bash_command='echo "Loading raw customer data"',
    )
    
    validate_data_schema = BashOperator(
        task_id='validate_data_schema',
        bash_command='echo "Validating schema"',
    )
    
    # Primera decisión: por calidad de datos
    branch_by_data_quality_task = BranchPythonOperator(
        task_id='branch_by_data_quality',
        python_callable=branch_by_data_quality,
    )
    
    process_premium_path = BashOperator(
        task_id='process_premium_path',
        bash_command='echo "Premium processing with advanced analytics"',
    )
    
    process_standard_path = BashOperator(
        task_id='process_standard_path',
        bash_command='echo "Standard processing"',
    )
    
    flag_for_manual_review = BashOperator(
        task_id='flag_for_manual_review',
        bash_command='echo "Flagging for human review"',
    )
    
    run_base_transformations = BashOperator(
        task_id='run_base_transformations',
        bash_command='echo "Running base transformations"',
        trigger_rule='none_failed_min_one_success',
    )
    
    # Segunda decisión: por volumen
    branch_by_record_count_task = BranchPythonOperator(
        task_id='branch_by_record_count',
        python_callable=branch_by_record_count,
    )
    
    use_spark_processing = BashOperator(
        task_id='use_spark_processing',
        bash_command='echo "Using Spark for millions of records"',
    )
    
    use_pandas_processing = BashOperator(
        task_id='use_pandas_processing',
        bash_command='echo "Using Pandas for thousands"',
    )
    
    use_simple_python = BashOperator(
        task_id='use_simple_python',
        bash_command='echo "Using vanilla Python"',
    )
    
    validate_processing_results = BashOperator(
        task_id='validate_processing_results',
        bash_command='echo "Validating results"',
        trigger_rule='none_failed_min_one_success',
    )
    
    check_business_metrics = BashOperator(
        task_id='check_business_metrics',
        bash_command='echo "Calculating business metrics"',
    )
    
    # Tercera decisión: por salud de métricas
    branch_by_metrics_health_task = BranchPythonOperator(
        task_id='branch_by_metrics_health',
        python_callable=branch_by_metrics_health,
    )
    
    load_to_production = BashOperator(
        task_id='load_to_production',
        bash_command='echo "Loading to production"',
    )
    
    create_incident_ticket = BashOperator(
        task_id='create_incident_ticket',
        bash_command='echo "Opening Jira ticket for investigation"',
    )
    
    update_monitoring_dashboard = BashOperator(
        task_id='update_monitoring_dashboard',
        bash_command='echo "Updating Grafana"',
        trigger_rule='none_failed_min_one_success',
    )
    
    # Paths finales
    archive_to_s3 = BashOperator(
        task_id='archive_to_s3',
        bash_command='echo "Archiving to S3"',
    )
    
    refresh_redis_cache = BashOperator(
        task_id='refresh_redis_cache',
        bash_command='echo "Invalidating cache"',
    )
    
    send_email_report = BashOperator(
        task_id='send_email_report',
        bash_command='echo "Sending summary report"',
    )
    
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')
    
    # Dependencies with Labels
    start >> ingest_customer_data >> validate_data_schema >> branch_by_data_quality_task
    
    # Primera decisión con labels
    branch_by_data_quality_task >> Label("Quality Excellent (>=95)") >> process_premium_path
    branch_by_data_quality_task >> Label("Quality Good (80-95)") >> process_standard_path
    branch_by_data_quality_task >> Label("Quality Poor (<80)") >> flag_for_manual_review
    
    [process_premium_path, process_standard_path, flag_for_manual_review] >> run_base_transformations
    
    # Segunda decisión con labels
    run_base_transformations >> branch_by_record_count_task
    branch_by_record_count_task >> Label("High Volume (100k+ records)") >> use_spark_processing
    branch_by_record_count_task >> Label("Medium Volume (10k-100k)") >> use_pandas_processing
    branch_by_record_count_task >> Label("Low Volume (<10k)") >> use_simple_python
    
    [use_spark_processing, use_pandas_processing, use_simple_python] >> validate_processing_results
    
    # Tercera decisión con labels
    validate_processing_results >> check_business_metrics >> branch_by_metrics_health_task
    branch_by_metrics_health_task >> Label("Metrics Healthy") >> load_to_production
    branch_by_metrics_health_task >> Label("Metrics Degraded - Investigation Required") >> create_incident_ticket
    
    [load_to_production, create_incident_ticket] >> update_monitoring_dashboard
    
    # Paths finales con labels
    update_monitoring_dashboard >> Label("Send to Data Lake") >> archive_to_s3
    update_monitoring_dashboard >> Label("Update Cache") >> refresh_redis_cache
    update_monitoring_dashboard >> Label("Notify Stakeholders") >> send_email_report
    
    [archive_to_s3, refresh_redis_cache, send_email_report] >> end
