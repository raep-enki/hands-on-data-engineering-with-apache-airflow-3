"""
# Multi-Region Data Sync

## Description
Sincroniza datos entre data centers en diferentes regiones geográficas
para mantener consistencia eventual y redundancia.

## Architecture
```
US-EAST (Primary) --> Replication --> [US-WEST, EU-CENTRAL, AP-SOUTH]
```

## Data Flow
1. Detectar cambios en primary (CDC)
2. Validar integridad de cambios
3. Replicar a regiones secundarias en paralelo
4. Verificar consistencia
5. Actualizar estado de sincronización

## Failure Handling
- Retry: 3 intentos con backoff exponencial
- Si falla región: Alerta + continuar con otras
- Si falla validación: Rollback + alerta crítica

## Notes
- **Latencia esperada**: < 5 minutos cross-region
- **RPO**: 5 minutos (Recovery Point Objective)
- **RTO**: 15 minutos (Recovery Time Objective)
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='documentation_with_diagrams',
    schedule='*/5 * * * *',  # Cada 5 minutos
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'documentation']
) as dag:
    dag.doc_md = __doc__
    
    start = EmptyOperator(task_id='start')
    
    detect_changes = BashOperator(
        task_id='detect_changes_cdc',
        bash_command='echo "🔍 Detectando cambios (CDC)"'
    )
    detect_changes.doc_md = """
    ### Change Data Capture (CDC)
    
    Detecta cambios en la base de datos primary usando Debezium.
    
    #### Tablas Monitoreadas
    | Tabla | Volume | Priority |
    |-------|--------|----------|
    | users | ~10K cambios/día | High |
    | orders | ~50K cambios/día | High |
    | products | ~500 cambios/día | Medium |
    | logs | ~1M cambios/día | Low |
    
    #### CDC Configuration
    ```json
    {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "snapshot.mode": "initial",
      "publication.name": "airflow_cdc",
      "slot.name": "airflow_slot"
    }
    ```
    
    #### Output Format
    Kafka topics: `cdc.{schema}.{table}`  
    Format: JSON with before/after states
    """
    
    validate = BashOperator(
        task_id='validate_changes',
        bash_command='echo "✅ Validando cambios"'
    )
    validate.doc_md = """
    ### Validate Changes
    
    Valida la integridad y consistencia de los cambios detectados.
    
    #### Validaciones
    1. **Schema Validation**
       - Verificar campos requeridos
       - Validar tipos de datos
       - Chequear constraints
    
    2. **Business Rules**
       - Orders: amount > 0
       - Users: valid email format
       - Products: stock >= 0
    
    3. **Referential Integrity**
       - Foreign keys válidas
       - No orphaned records
    
    #### Actions on Failure
    - Rechazar cambio inválido
    - Log detallado del error
    - Incrementar contador de errores
    - Si > 100 errores/hora: Alerta crítica
    """
    
    # Replicación paralela a regiones
    sync_us_west = BashOperator(
        task_id='sync_to_us_west',
        bash_command='echo "🌎 Sincronizando a US-WEST"'
    )
    sync_us_west.doc_md = """
    ### Sync to US-WEST Region
    
    **Endpoint**: `postgres-us-west.company.com:5432`  
    **Database**: `prod_replica`  
    **Method**: Logical Replication  
    **Latency**: 20-50ms
    
    #### Monitoring
    - Lag (replication_lag): < 5 segundos
    - Throughput: monitored via CloudWatch
    - Errors: logged to `sync_errors.us_west`
    """
    
    sync_eu_central = BashOperator(
        task_id='sync_to_eu_central',
        bash_command='echo "🌍 Sincronizando a EU-CENTRAL"'
    )
    sync_eu_central.doc_md = """
    ### Sync to EU-CENTRAL Region
    
    **Endpoint**: `postgres-eu-central.company.com:5432`  
    **Database**: `prod_replica`  
    **Method**: Logical Replication  
    **Latency**: 80-120ms
    
    #### GDPR Compliance
    - PII data encrypted in transit (TLS 1.3)
    - PII data encrypted at rest (AES-256)
    - Audit log enabled
    - Data residency: EU region only
    """
    
    sync_ap_south = BashOperator(
        task_id='sync_to_ap_south',
        bash_command='echo "🌏 Sincronizando a AP-SOUTH"'
    )
    sync_ap_south.doc_md = """
    ### Sync to AP-SOUTH Region
    
    **Endpoint**: `postgres-ap-south.company.com:5432`  
    **Database**: `prod_replica`  
    **Method**: Logical Replication  
    **Latency**: 150-200ms
    
    #### Notes
    - Highest latency due to distance
    - Monitor for replication lag
    - Scheduled maintenance: Sundays 00:00-02:00 SGT
    """
    
    verify_consistency = BashOperator(
        task_id='verify_consistency',
        bash_command='echo "🔍 Verificando consistencia"'
    )
    verify_consistency.doc_md = """
    ### Verify Cross-Region Consistency
    
    Verifica que todas las regiones tengan los datos sincronizados.
    
    #### Consistency Checks
    1. **Row Counts**
       - Comparar # de filas por tabla
       - Tolerancia: ±10 filas (por eventual consistency)
    
    2. **Checksum Validation**
       - Calcular hash de últimas 1000 filas
       - Comparar entre regiones
       - Alertar si differs > 5%
    
    3. **Timestamp Verification**
       - MAX(updated_at) por tabla
       - Lag máximo permitido: 5 minutos
    
    #### Reconciliation
    Si inconsistencia detectada:
    - Trigger full table sync para tabla afectada
    - Notificar al equipo
    - Log en `consistency_issues` table
    """
    
    update_status = BashOperator(
        task_id='update_sync_status',
        bash_command='echo "📊 Actualizando estado"'
    )
    update_status.doc_md = """
    ### Update Sync Status Dashboard
    
    Actualiza métricas de sincronización en el dashboard de monitoreo.
    
    #### Métricas Publicadas
    - Cambios replicados (count)
    - Lag por región (segundos)
    - Errores por región (count)
    - Throughput (changes/minute)
    - Success rate (%)
    
    #### Dashboards
    - Grafana: `https://monitoring.company.com/d/replication`
    - Datadog: `dashboard:replication-status`
    """
    
    end = EmptyOperator(task_id='end')
    
    start >> detect_changes >> validate
    validate >> [sync_us_west, sync_eu_central, sync_ap_south]
    [sync_us_west, sync_eu_central, sync_ap_south] >> verify_consistency
    verify_consistency >> update_status >> end
