"""
Backfill - Validación de Datos

Demuestra cómo agregar validaciones que detecten cuándo
un backfill es necesario y qué períodos necesitan reprocesamiento.

Útil para identificar automáticamente gaps en los datos.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='backfill_with_validation',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dag_runs', 'backfill']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Verificar que los datos de fuente existen
    check_source_exists = BashOperator(
        task_id='check_source_exists',
        bash_command="""
        echo "🔍 Verificando existencia de datos source para {{ ds }}"
        echo "📂 Checking: /source/data/{{ ds }}/"
        echo "✅ Source data found"
        """
    )
    
    # Validar calidad de datos source
    validate_source_quality = BashOperator(
        task_id='validate_source_quality',
        bash_command="""
        echo "✅ Validando calidad de source para {{ ds }}"
        echo "📊 Row count: Expected vs Actual"
        echo "🔢 Null checks: No unexpected nulls"
        echo "📈 Value ranges: Within expected bounds"
        """
    )
    
    # Verificar si datos ya fueron procesados (para evitar duplicados)
    check_already_processed = BashOperator(
        task_id='check_already_processed',
        bash_command="""
        echo "🔍 Verificando si {{ ds }} ya fue procesado"
        echo "SELECT COUNT(*) FROM target WHERE date = '{{ ds }}'"
        if [ processed ]; then
            echo "⚠️ Datos ya existen - será sobrescrito (idempotencia)"
        else
            echo "✅ Fecha no procesada - proceder con carga"
        fi
        """
    )
    
    # Procesamiento
    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "📥 Extrayendo datos para {{ ds }}"'
    )
    
    transform = BashOperator(
        task_id='transform_data',
        bash_command='echo "⚙️ Transformando datos para {{ ds }}"'
    )
    
    load = BashOperator(
        task_id='load_data',
        bash_command='echo "📤 Cargando datos para {{ ds }}"'
    )
    
    # Validaciones post-carga
    validate_row_count = BashOperator(
        task_id='validate_row_count',
        bash_command="""
        echo "📊 Validando row count para {{ ds }}"
        echo "Expected: 10000, Actual: 10000 ✅"
        """
    )
    
    validate_data_completeness = BashOperator(
        task_id='validate_data_completeness',
        bash_command="""
        echo "✅ Validando completitud para {{ ds }}"
        echo "🔢 No missing critical fields"
        echo "📈 All expected categories present"
        echo "🕐 Timestamps within expected range"
        """
    )
    
    validate_business_rules = BashOperator(
        task_id='validate_business_rules',
        bash_command="""
        echo "✅ Validando business rules para {{ ds }}"
        echo "💰 Revenue = price * quantity"
        echo "📊 Aggregations sum correctly"
        echo "🔗 Foreign keys valid"
        """
    )
    
    # Registrar que el período fue procesado exitosamente
    record_completion = BashOperator(
        task_id='record_completion',
        bash_command="""
        echo "✅ Registrando completitud para {{ ds }}"
        echo "INSERT INTO processing_log VALUES ('{{ ds }}', '{{ ts }}', 'SUCCESS')"
        """
    )
    
    end = EmptyOperator(task_id='end')
    
    # Dependencies
    start >> check_source_exists >> validate_source_quality >> check_already_processed
    check_already_processed >> extract >> transform >> load
    load >> [validate_row_count, validate_data_completeness, validate_business_rules]
    [validate_row_count, validate_data_completeness, validate_business_rules] >> record_completion >> end

dag.doc_md = """
# Backfill con Validación de Datos

Pipeline que detecta cuándo es necesario hacer backfill mediante validaciones.

## Tipos de validaciones:

### 1. Pre-procesamiento
**Objetivo:** Detectar si es seguro procesar

```sql
-- Verificar que source data existe
SELECT COUNT(*) FROM source_data WHERE date = '{{ ds }}'
HAVING COUNT(*) > 0

-- Validar calidad mínima
SELECT 
    COUNT(*) as total,
    COUNT(NULLIF(critical_field, '')) as non_null
FROM source_data 
WHERE date = '{{ ds }}'
HAVING COUNT(*) > 1000 AND non_null = total
```

**Acción si falla:** 
- Task falla → DAG marca como failed
- Alerta al equipo que datos de source no están listos
- Backfill automático cuando se corrija

### 2. Durante procesamiento
**Objetivo:** Validar integridad de transformaciones

```python
# Comparar row counts
source_count = extract_task.get_row_count()
target_count = load_task.get_row_count()
assert source_count == target_count, "Row count mismatch!"
```

### 3. Post-procesamiento
**Objetivo:** Confirmar que datos cargados son correctos

```sql
-- Validar agregaciones
SELECT 
    COUNT(*) as rows,
    SUM(revenue) as total_revenue,
    MIN(date) as min_date,
    MAX(date) as max_date
FROM target_table
WHERE date = '{{ ds }}'
HAVING 
    rows BETWEEN 1000 AND 100000 AND
    total_revenue > 0 AND
    min_date = '{{ ds }}' AND
    max_date = '{{ ds }}'
```

## Data Quality Checks comunes:

### Completitud (Completeness)
```python
# ¿Están presentes todos los campos requeridos?
SELECT COUNT(*) 
FROM target 
WHERE date = '{{ ds }}' 
  AND (required_field IS NULL OR required_field = '')
HAVING COUNT(*) = 0  # Debería ser 0 nulls
```

### Unicidad (Uniqueness)
```python
# ¿Hay duplicados?
SELECT id, COUNT(*) as cnt
FROM target
WHERE date = '{{ ds }}'
GROUP BY id
HAVING cnt > 1  # No debería retornar filas
```

### Validez (Validity)
```python
# ¿Los valores están en rangos válidos?
SELECT COUNT(*)
FROM target
WHERE date = '{{ ds }}'
  AND (
    age < 0 OR age > 150 OR
    revenue < 0 OR
    email NOT LIKE '%@%'
  )
HAVING COUNT(*) = 0  # No valores inválidos
```

### Consistencia (Consistency)
```python
# ¿Las agregaciones son consistentes?
SELECT 
    SUM(line_item_total) as sum_lines,
    SUM(order_total) as sum_orders
FROM target
WHERE date = '{{ ds }}'
HAVING ABS(sum_lines - sum_orders) < 0.01  # Diferencia < 1 centavo
```

### Frescura (Freshness)
```python
# ¿Los datos son recientes?
SELECT MAX(updated_at) as last_update
FROM target
WHERE date = '{{ ds }}'
HAVING last_update >= '{{ execution_date }}'
```

## Detectar períodos que necesitan backfill:

### Query para identificar gaps
```sql
-- Encontrar fechas faltantes en un rango
WITH date_series AS (
    SELECT generate_series(
        '2021-01-01'::date,
        CURRENT_DATE,
        '1 day'::interval
    )::date as expected_date
),
processed_dates AS (
    SELECT DISTINCT date 
    FROM target_table
)
SELECT expected_date as missing_date
FROM date_series
LEFT JOIN processed_dates ON date_series.expected_date = processed_dates.date
WHERE processed_dates.date IS NULL
ORDER BY expected_date
```

**Output:**
```
missing_date
------------
2021-01-15
2021-01-23
2021-02-01
```

### Script para backfill automático de gaps
```bash
#!/bin/bash
# Obtener fechas faltantes
missing_dates=$(psql -t -c "SELECT missing_date FROM gap_detection")

# Backfill cada fecha
for date in $missing_dates; do
    echo "Backfilling $date"
    airflow dags backfill \\
        --start-date $date \\
        --end-date $date \\
        backfill_with_validation
done
```

## Manejo de validación fallida:

### Opción 1: Task Sensor
```python
# Esperar hasta que datos sean válidos
wait_for_valid_source = ExternalTaskSensor(
    task_id='wait_for_source',
    external_dag_id='source_validation_dag',
    external_task_id='validation_passed'
)
```

### Opción 2: Retry con backoff
```python
# Reintentar si datos no están listos
validate_task = PythonOperator(
    task_id='validate',
    retries=5,
    retry_delay=timedelta(minutes=10)
)
```

### Opción 3: Branch en error
```python
# Tomar acción alternativa si validación falla
validation >> [process_normal, notify_and_skip]
```

## Logging de validaciones:

### Tabla de audit log
```sql
CREATE TABLE processing_audit (
    date DATE,
    run_timestamp TIMESTAMP,
    status VARCHAR(20),
    validation_checks JSONB,
    error_message TEXT
)

-- Insertar resultado
INSERT INTO processing_audit VALUES (
    '{{ ds }}',
    '{{ ts }}',
    'SUCCESS',
    '{"row_count": 10000, "nulls": 0, "duplicates": 0}'::jsonb,
    NULL
)
```

## Best practices:

1. **Fail fast**: Validar source antes de procesar
2. **Validaciones granulares**: Múltiples checks pequeños, no uno grande
3. **Alertas apropiadas**: Solo alertar en problemas críticos
4. **Registro completo**: Log de todas las validaciones
5. **Idempotencia**: Backfill debe poder reejecutarse
"""
