"""
Backfill - Reprocesamiento Selectivo

Demuestra cómo diseñar un DAG que facilita backfill selectivo
de solo las tareas que necesitan reprocesarse.

Útil cuando el bug afectó solo una parte del pipeline.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='backfill_selective_reprocess',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dag_runs', 'backfill']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Extracción (raramente necesita reprocessing)
    extract_sales = BashOperator(
        task_id='extract_sales',
        bash_command="""
        echo "📥 Extrayendo sales para {{ ds }}"
        echo "💾 Guardando en: staging/sales_{{ ds_nodash }}.parquet"
        """
    )
    
    extract_inventory = BashOperator(
        task_id='extract_inventory',
        bash_command="""
        echo "📥 Extrayendo inventory para {{ ds }}"
        echo "💾 Guardando en: staging/inventory_{{ ds_nodash }}.parquet"
        """
    )
    
    checkpoint_extract = EmptyOperator(task_id='checkpoint_extract')
    
    # Transformaciones (pueden tener bugs que necesitan fix)
    transform_sales = BashOperator(
        task_id='transform_sales',
        bash_command="""
        echo "⚙️ Transformando sales para {{ ds }}"
        echo "📂 Input: staging/sales_{{ ds_nodash }}.parquet"
        echo "📂 Output: processed/sales_{{ ds_nodash }}.parquet"
        """
    )
    
    transform_inventory = BashOperator(
        task_id='transform_inventory',
        bash_command="""
        echo "⚙️ Transformando inventory para {{ ds }}"
        echo "📂 Input: staging/inventory_{{ ds_nodash }}.parquet"
        echo "📂 Output: processed/inventory_{{ ds_nodash }}.parquet"
        """
    )
    
    checkpoint_transform = EmptyOperator(task_id='checkpoint_transform')
    
    # Agregaciones (dependen de transformaciones)
    aggregate_metrics = BashOperator(
        task_id='aggregate_metrics',
        bash_command="""
        echo "📊 Agregando metrics para {{ ds }}"
        echo "📂 Inputs: processed/sales_{{ ds_nodash }}.parquet + inventory_{{ ds_nodash }}.parquet"
        echo "📂 Output: metrics/daily_{{ ds_nodash }}.parquet"
        """
    )
    
    # Carga final
    load_warehouse = BashOperator(
        task_id='load_warehouse',
        bash_command="""
        echo "📤 Cargando a warehouse para {{ ds }}"
        echo "🗑️ DELETE FROM warehouse WHERE date = '{{ ds }}'"
        echo "📥 INSERT INTO warehouse FROM metrics/daily_{{ ds_nodash }}.parquet"
        """
    )
    
    end = EmptyOperator(task_id='end')
    
    # Dependencies con checkpoints claros
    start >> [extract_sales, extract_inventory] >> checkpoint_extract
    checkpoint_extract >> [transform_sales, transform_inventory] >> checkpoint_transform
    checkpoint_transform >> aggregate_metrics >> load_warehouse >> end

dag.doc_md = """
# Backfill Selectivo

Estrategia para reprocesar solo las partes del pipeline que necesitan fix.

## Escenarios comunes:

### Escenario 1: Bug en transformación de sales
```bash
# Solo reprocesar transform_sales hacia adelante
airflow tasks clear \\
    --start-date 2021-01-01 \\
    --end-date 2021-01-31 \\
    --downstream \\  # Incluir tareas dependientes
    backfill_selective_reprocess \\
    --task-regex "transform_sales|aggregate_metrics|load_warehouse"
```

**Resultado:**
- ✅ extract_sales: No se reejecuta (datos ya están en staging/)
- 🔄 transform_sales: Reprocessa con fix
- 🔄 aggregate_metrics: Recalcula con datos corregidos
- 🔄 load_warehouse: Recarga datos actualizados

### Escenario 2: Bug solo en agregación
```bash
# Solo desde aggregate hacia adelante
airflow tasks clear \\
    --start-date 2021-01-01 \\
    --end-date 2021-01-31 \\
    --downstream \\
    backfill_selective_reprocess \\
    --task-regex "aggregate_metrics|load_warehouse"
```

**Resultado:**
- ✅ extracts: No se tocan
- ✅ transforms: No se tocan
- 🔄 aggregate: Recalcula con fix
- 🔄 load: Recarga

### Escenario 3: Necesitas reextraer todo
```bash
# Full reprocess desde el inicio
airflow dags backfill \\
    --start-date 2021-01-01 \\
    --end-date 2021-01-31 \\
    --rerun-failed-tasks \\
    backfill_selective_reprocess
```

## Principios de diseño para backfill selectivo:

### 1. Persistencia de datos intermedios
```python
# Guardar outputs de cada etapa en storage
extract >> save_to_s3_staging
transform >> save_to_s3_processed
aggregate >> save_to_s3_metrics
```

**Beneficio:** Etapas anteriores no necesitan reejecutarse.

### 2. Checkpoints claros
```python
# Usar EmptyOperator como checkpoints
checkpoint_extract = EmptyOperator(task_id='checkpoint_extract')
checkpoint_transform = EmptyOperator(task_id='checkpoint_transform')
```

**Beneficio:** Visualización clara de etapas del pipeline.

### 3. Naming consistency
```python
# Pattern: <stage>_<entity>
extract_sales, extract_inventory
transform_sales, transform_inventory
```

**Beneficio:** Fácil de seleccionar con task-regex.

### 4. Dependencias explícitas
```python
# Claramente definir qué depende de qué
[extract1, extract2] >> checkpoint
checkpoint >> [transform1, transform2]
```

**Beneficio:** Downstream/upstream clears funcionan correctamente.

## Comandos útiles de Airflow CLI:

### Clear tasks por pattern
```bash
# Clear todas las tareas de transform
airflow tasks clear \\
    --task-regex "transform_.*" \\
    --start-date 2021-01-01 \\
    backfill_selective_reprocess
```

### Clear con confirmación
```bash
# Dry run primero para ver qué se afectará
airflow tasks clear \\
    --dry-run \\
    --task-regex "aggregate.*" \\
    backfill_selective_reprocess
```

### Clear solo failed tasks
```bash
# Solo reprocesar lo que falló
airflow dags backfill \\
    --rerun-failed-tasks \\
    --only-failed \\
    backfill_selective_reprocess
```

## Anti-patterns a evitar:

### ❌ Limpiar datos intermedios inmediatamente
```python
# MAL: No puedes hacer backfill selectivo
extract >> transform >> delete_staging >> load
```

### ❌ Todo en una sola tarea
```python
# MAL: Necesitas reprocesar todo aunque solo una parte tenga bug
single_task_does_everything()
```

### ❌ Dependencies poco claras
```python
# MAL: Difícil saber qué afecta qué
task1 >> task3
task2 >> task4
task1 >> task4
```

## Best practices:

1. **Separar en etapas lógicas**: Extract, Transform, Load
2. **Guardar outputs intermedios**: Para poder saltar etapas
3. **Idempotencia en cada etapa**: Cada tarea puede reejecutarse
4. **Checkpoints visuales**: EmptyOperator entre etapas
5. **Naming convention**: Consistente para task-regex
"""
