"""
Backfill - Idempotencia

Demuestra la importancia de diseñar DAGs idempotentes
para que el backfill pueda ejecutarse múltiples veces
con el mismo resultado.

Un pipeline idempotente produce el mismo resultado
sin importar cuántas veces se ejecute.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='backfill_idempotent_pipeline',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dag_runs', 'backfill']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # BUENA PRÁCTICA: DELETE + INSERT para idempotencia
    clear_partition = BashOperator(
        task_id='clear_partition',
        bash_command="""
        echo "🗑️ Limpiando partición existente para {{ ds }}"
        echo "DELETE FROM sales WHERE date = '{{ ds }}'"
        """
    )
    
    extract = BashOperator(
        task_id='extract_source',
        bash_command="""
        echo "📥 Extrayendo datos de fuente para {{ ds }}"
        echo "SELECT * FROM source_sales WHERE date = '{{ ds }}'"
        """
    )
    
    # Transformación determinística
    transform = BashOperator(
        task_id='transform_deterministic',
        bash_command="""
        echo "⚙️ Transformación idempotente para {{ ds }}"
        echo "✅ Mismos inputs = mismos outputs"
        echo "✅ Sin timestamps de ejecución en los datos"
        echo "✅ Sin dependencies de estado previo"
        """
    )
    
    # INSERT con partición limpia
    load = BashOperator(
        task_id='load_partition',
        bash_command="""
        echo "📤 Cargando datos a partición {{ ds }}"
        echo "INSERT INTO sales (date, ...) VALUES ..."
        echo "✅ Partición limpia garantiza idempotencia"
        """
    )
    
    validate = BashOperator(
        task_id='validate_result',
        bash_command="""
        echo "✅ Validando resultado para {{ ds }}"
        echo "📊 Record count, checksums, data quality"
        """
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> clear_partition >> extract >> transform >> load >> validate >> end

dag.doc_md = """
# Pipeline Idempotente para Backfill

**Idempotencia**: Propiedad de que ejecutar una operación múltiples veces
produce el mismo resultado que ejecutarla una vez.

## Por qué importa para backfill:

### Problema sin idempotencia:
```python
# ❌ MAL: Acumula duplicados en cada ejecución
INSERT INTO sales VALUES (...)
# Ejecutar backfill 3 veces = 3x los datos
```

### Solución idempotente:
```python
# ✅ BIEN: Siempre mismo resultado
DELETE FROM sales WHERE date = '{{ ds }}'
INSERT INTO sales VALUES (...)
# Ejecutar backfill 3 veces = mismos datos
```

## Patrones de idempotencia:

### 1. DELETE + INSERT (recomendado)
```sql
-- Limpiar partición específica
DELETE FROM target WHERE date = '{{ ds }}'
-- Cargar datos frescos
INSERT INTO target SELECT ...
```

### 2. TRUNCATE + INSERT (para full refresh)
```sql
-- Para tablas sin particiones
TRUNCATE TABLE target
INSERT INTO target SELECT ...
```

### 3. MERGE/UPSERT (para updates)
```sql
-- Insertar nuevos o actualizar existentes
MERGE INTO target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

### 4. CREATE OR REPLACE (para tablas temporales)
```sql
CREATE OR REPLACE TABLE temp_{{ ds_nodash }} AS
SELECT ...
```

## Anti-patrones a evitar:

### ❌ Usar timestamp de ejecución en datos
```python
# MAL: Cada backfill tendrá timestamp diferente
INSERT INTO logs VALUES (now(), ...)
```
```python
# BIEN: Usar logical_date
INSERT INTO logs VALUES ('{{ logical_date }}', ...)
```

### ❌ Acumular sin limpiar
```python
# MAL: Duplica datos en cada run
INSERT INTO metrics SELECT ...
```
```python
# BIEN: Limpiar primero
DELETE FROM metrics WHERE date = '{{ ds }}'
INSERT INTO metrics SELECT ...
```

### ❌ Depender de estado previo
```python
# MAL: Necesita run anterior para calcular
UPDATE sales SET total = total + new_value
```
```python
# BIEN: Calcular desde fuente
UPDATE sales SET total = (SELECT SUM(...) FROM source)
```

## Beneficios de idempotencia:

1. **Backfill seguro**: Reprocesar datos sin miedo a duplicados
2. **Recovery fácil**: Reejecutar DAG fallido sin cleanup manual
3. **Testing confiable**: Mismos inputs = mismos outputs siempre
4. **Debugging simple**: Reproducir problemas consistentemente
5. **CI/CD friendly**: Tests determinísticos

## Checklist para DAG idempotente:

- [ ] Limpiar datos existentes antes de cargar nuevos
- [ ] No usar timestamps de ejecución en datos de negocio
- [ ] Transformaciones determinísticas (sin random, sin now())
- [ ] No depender de estado de runs anteriores
- [ ] Validar que reejecutar produce mismo resultado
- [ ] Usar particiones para aislar períodos de tiempo
"""
