"""
Backfill - Particionamiento Eficiente

Demuestra cómo usar particiones para hacer backfill más eficiente
y permitir procesamiento paralelo de múltiples períodos.

Las particiones permiten aislar datos por período de tiempo.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='backfill_partitioned_efficiently',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=5,  # Permite múltiples runs en paralelo
    tags=['example', 'backfill']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Crear partición para el día si no existe
    create_partition = BashOperator(
        task_id='create_partition',
        bash_command="""
        echo "📁 Creando partición para {{ ds }}"
        echo "ALTER TABLE sales ADD IF NOT EXISTS PARTITION (date='{{ ds }}')"
        """
    )
    
    # Limpiar partición específica (no toda la tabla)
    truncate_partition = BashOperator(
        task_id='truncate_partition',
        bash_command="""
        echo "🗑️ Truncando partición {{ ds }}"
        echo "ALTER TABLE sales DROP PARTITION (date='{{ ds }}')"
        echo "ALTER TABLE sales ADD PARTITION (date='{{ ds }}')"
        echo "✅ Partición limpia y lista"
        """
    )
    
    # Extract solo para este día
    extract_daily = BashOperator(
        task_id='extract_daily',
        bash_command="""
        echo "📥 Extrayendo datos para {{ ds }}"
        echo "SELECT * FROM source WHERE date = '{{ ds }}'"
        echo "💾 Output: s3://data/{{ ds }}/raw/"
        """
    )
    
    # Transform solo este día
    transform_daily = BashOperator(
        task_id='transform_daily',
        bash_command="""
        echo "⚙️ Transformando datos para {{ ds }}"
        echo "📂 Input: s3://data/{{ ds }}/raw/"
        echo "📂 Output: s3://data/{{ ds }}/processed/"
        """
    )
    
    # Load a partición específica
    load_to_partition = BashOperator(
        task_id='load_to_partition',
        bash_command="""
        echo "📤 Cargando a partición {{ ds }}"
        echo "LOAD DATA INPATH 's3://data/{{ ds }}/processed/'"
        echo "INTO TABLE sales PARTITION (date='{{ ds }}')"
        echo "✅ Datos cargados en partición aislada"
        """
    )
    
    # Validar solo esta partición
    validate_partition = BashOperator(
        task_id='validate_partition',
        bash_command="""
        echo "✅ Validando partición {{ ds }}"
        echo "SELECT COUNT(*) FROM sales WHERE date = '{{ ds }}'"
        echo "📊 Row count validation passed"
        """
    )
    
    # Actualizar metadatos de partición
    update_partition_metadata = BashOperator(
        task_id='update_partition_metadata',
        bash_command="""
        echo "📝 Actualizando metadata para partición {{ ds }}"
        echo "MSCK REPAIR TABLE sales"
        echo "ANALYZE TABLE sales PARTITION (date='{{ ds }}') COMPUTE STATISTICS"
        """
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> create_partition >> truncate_partition >> extract_daily
    extract_daily >> transform_daily >> load_to_partition
    load_to_partition >> validate_partition >> update_partition_metadata >> end

dag.doc_md = """
# Backfill con Particionamiento Eficiente

Estrategia de particionamiento para backfill rápido y paralelo.

## Ventajas del particionamiento:

### 1. Procesamiento paralelo
```python
# Configurar max_active_runs para paralelismo
dag = DAG(
    dag_id='my_dag',
    max_active_runs=10,  # 10 días procesándose simultáneamente
    ...
)
```

**Resultado:**
- Backfill de 30 días puede completarse en ~3 días (10x más rápido)
- Cada DAG run procesa su partición independientemente
- No hay contención de recursos entre períodos

### 2. Aislamiento de datos
```sql
-- Cada día en su propia partición
/data/sales/year=2021/month=01/day=01/
/data/sales/year=2021/month=01/day=02/
/data/sales/year=2021/month=01/day=03/
```

**Beneficios:**
- Backfill del día 15 no afecta día 16
- Fallos aislados a una partición
- Fácil eliminar/reprocesar períodos específicos

### 3. Queries más rápidas
```sql
-- Query solo escanea particiones relevantes
SELECT * FROM sales 
WHERE date BETWEEN '2021-01-01' AND '2021-01-31'
-- Solo escanea 31 particiones, no toda la tabla
```

### 4. Gestión de datos
```sql
-- Eliminar datos antiguos eficientemente
ALTER TABLE sales DROP PARTITION (date='2020-01-01')
-- Mucho más rápido que DELETE
```

## Esquemas de particionamiento:

### Por fecha (más común)
```sql
-- Partición diaria
PARTITIONED BY (date DATE)

-- Partición jerárquica
PARTITIONED BY (year INT, month INT, day INT)

-- Ventaja: Fácil de razonar, natural para series de tiempo
```

### Por región + fecha
```sql
-- Multi-dimensional
PARTITIONED BY (region STRING, date DATE)

-- Permite procesamiento por región en paralelo
-- Ejemplo: us-east, us-west, eu-west procesándose simultáneamente
```

### Hive-style partitioning
```
# Formato en filesystem
s3://bucket/table/year=2021/month=01/day=15/data.parquet
s3://bucket/table/year=2021/month=01/day=16/data.parquet
```

## Implementación en diferentes sistemas:

### Hive/Presto/Athena
```sql
-- Crear tabla particionada
CREATE TABLE sales (
    order_id INT,
    revenue DECIMAL
)
PARTITIONED BY (date DATE)
STORED AS PARQUET

-- Agregar partición
ALTER TABLE sales ADD PARTITION (date='2021-01-15')

-- Cargar datos a partición
INSERT INTO sales PARTITION (date='2021-01-15')
SELECT order_id, revenue FROM source WHERE date = '2021-01-15'
```

### BigQuery
```sql
-- Tabla particionada por fecha
CREATE TABLE sales (
    order_id INT64,
    revenue NUMERIC,
    date DATE
)
PARTITION BY date

-- BigQuery auto-maneja particiones en INSERT
INSERT INTO sales VALUES (123, 99.99, '2021-01-15')
```

### Spark
```python
# Escribir con particionamiento
df.write \\
    .partitionBy("date") \\
    .mode("overwrite") \\
    .parquet("s3://bucket/sales/")

# Leer partición específica
df = spark.read.parquet("s3://bucket/sales/date=2021-01-15")
```

### PostgreSQL (partition by range)
```sql
-- Tabla padre
CREATE TABLE sales (
    order_id INT,
    revenue DECIMAL,
    date DATE
) PARTITION BY RANGE (date)

-- Crear particiones
CREATE TABLE sales_2021_01 PARTITION OF sales
FOR VALUES FROM ('2021-01-01') TO ('2021-02-01')

CREATE TABLE sales_2021_02 PARTITION OF sales
FOR VALUES FROM ('2021-02-01') TO ('2021-03-01')
```

## Backfill con particiones:

### Comando para backfill paralelo
```bash
# Backfill 30 días con 10 runs en paralelo
airflow dags backfill \\
    --start-date 2021-01-01 \\
    --end-date 2021-01-31 \\
    --max-active-runs 10 \\
    backfill_partitioned_efficiently
```

### Monitoring de backfill paralelo
```bash
# Ver runs activos
airflow dags list-runs \\
    --dag-id backfill_partitioned_efficiently \\
    --state running

# Output:
# 2021-01-01: running
# 2021-01-02: running
# 2021-01-03: running
# ... (10 total)
```

### Backfill de particiones faltantes
```sql
-- Detectar particiones faltantes
WITH expected_dates AS (
    SELECT generate_series(
        '2021-01-01'::date,
        '2021-01-31'::date,
        '1 day'
    )::date as date
)
SELECT e.date as missing_partition
FROM expected_dates e
LEFT JOIN (
    SELECT DISTINCT date FROM sales
) s ON e.date = s.date
WHERE s.date IS NULL

-- Backfill solo las particiones faltantes
```

## Anti-patterns:

### ❌ Particiones muy granulares
```sql
-- MAL: Partición por hora
PARTITIONED BY (year, month, day, hour)
-- Resultado: Miles de particiones pequeñas
-- Overhead de metadata excesivo
```

### ❌ Cargar todo y luego particionar
```sql
-- MAL: Cargar primero, particionar después
INSERT INTO sales_staging SELECT * FROM source  -- Sin particionar
-- Luego mover a particiones
-- Ineficiente y usa espacio doble
```

### ❌ No limpiar partición antes de cargar
```sql
-- MAL: Acumular sin limpiar
INSERT INTO sales PARTITION (date='2021-01-15')
-- Backfill duplicará datos
```

## Best practices:

1. **Granularidad apropiada**: Diaria para la mayoría de casos
2. **Truncate antes de cargar**: Idempotencia
3. **Max active runs**: Limitar paralelismo para no saturar
4. **Partition pruning**: Queries siempre filtran por partición
5. **Maintenance**: Compactar particiones pequeñas periódicamente
6. **Monitoring**: Alertar en particiones faltantes o muy grandes
"""
