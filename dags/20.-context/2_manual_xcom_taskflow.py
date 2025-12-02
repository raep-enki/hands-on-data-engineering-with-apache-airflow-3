"""
TaskFlow - XCom Push/Pull Manual con TaskFlow

Aunque TaskFlow maneja XCom automáticamente con returns,
a veces necesitas push/pull manual para keys personalizados.

Útil para compartir múltiples valores sin retornar objetos grandes.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator


@task
def producer_task(ti):
    """Produce múltiples valores usando XCom push"""
    print("🏭 Produciendo datos...")
    
    # Return automático (XCom default)
    data = {"main_result": "primary data"}
    
    # XCom manual para valores adicionales
    ti.xcom_push(key='metadata', value={'version': '1.0', 'source': 'api'})
    ti.xcom_push(key='stats', value={'count': 100, 'size_mb': 5.2})
    ti.xcom_push(key='timestamp', value=str(datetime.datetime.now()))
    
    print("✅ Datos producidos:")
    print(f"  - Main: {data}")
    print("  - Metadata, stats, timestamp en XCom")
    
    return data


@task
def consumer_task(main_data: dict, ti):
    """Consume datos: return automático + XCom manual"""
    print("📥 Consumiendo datos...")
    
    # main_data viene del return de producer_task (automático)
    print(f"  - Main data (auto): {main_data}")
    
    # Pull manual de XCom keys adicionales
    metadata = ti.xcom_pull(task_ids='producer_task', key='metadata')
    stats = ti.xcom_pull(task_ids='producer_task', key='stats')
    timestamp = ti.xcom_pull(task_ids='producer_task', key='timestamp')
    
    print(f"  - Metadata (manual): {metadata}")
    print(f"  - Stats (manual): {stats}")
    print(f"  - Timestamp (manual): {timestamp}")
    
    # Combinar todo
    result = {
        **main_data,
        'metadata': metadata,
        'stats': stats,
        'processed_at': timestamp
    }
    
    return result


@task
def multi_source_consumer(ti):
    """Consume XComs de múltiples tareas"""
    print("🔀 Consumiendo de múltiples fuentes...")
    
    # Pull de la tarea producer
    metadata = ti.xcom_pull(task_ids='producer_task', key='metadata')
    
    # Pull del return de consumer (default key='return_value')
    consumer_result = ti.xcom_pull(task_ids='consumer_task')
    
    print(f"  - Producer metadata: {metadata}")
    print(f"  - Consumer result: {consumer_result}")
    
    return {
        'producer_meta': metadata,
        'consumer_data': consumer_result
    }


with DAG(
    dag_id='taskflow_xcom_manual',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'context']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Pipeline
    main = producer_task()
    processed = consumer_task(main)
    final = multi_source_consumer()
    
    end = EmptyOperator(task_id='end')
    
    start >> main
    processed >> final >> end

dag.doc_md = """
# XCom Manual con TaskFlow

TaskFlow maneja XCom automáticamente, pero puedes usar push/pull manual.

## Return automático (recomendado):

```python
@task
def my_task():
    data = {"result": 100}
    return data  # Automáticamente push a XCom

@task
def next_task(data: dict):  # Automáticamente pull del return
    print(data)  # {"result": 100}
```

## Push/Pull manual (cuando necesario):

```python
@task
def producer(ti):
    # Return para resultado principal
    main_data = {"result": 100}
    
    # Push manual para datos adicionales
    ti.xcom_push(key='metadata', value={'version': '1.0'})
    ti.xcom_push(key='stats', value={'count': 50})
    
    return main_data

@task
def consumer(main_data: dict, ti):  # main_data del return
    # Pull manual para datos adicionales
    metadata = ti.xcom_pull(task_ids='producer', key='metadata')
    stats = ti.xcom_pull(task_ids='producer', key='stats')
    
    print(main_data)  # {"result": 100}
    print(metadata)   # {"version": "1.0"}
    print(stats)      # {"count": 50}
```

## Cuándo usar push/pull manual:

### ✅ Usar manual cuando:
1. **Múltiples valores**: Compartir varios valores sin crear dict grande
2. **Metadata**: Datos auxiliares separados del resultado principal
3. **Keys semánticos**: 'config', 'metrics', 'validation_results'
4. **Pull condicional**: Solo pull si necesario

### ❌ Evitar manual cuando:
1. **Un solo valor**: Usa return
2. **Datos grandes**: XCom no es para archivos grandes
3. **Complejidad innecesaria**: Return es más simple

## XCom Pull patterns:

### Pull de tarea específica
```python
@task
def consumer(ti):
    data = ti.xcom_pull(task_ids='specific_task')
    metadata = ti.xcom_pull(task_ids='specific_task', key='metadata')
```

### Pull de múltiples tareas
```python
@task
def aggregator(ti):
    # Pull de varias tareas paralelas
    results = ti.xcom_pull(task_ids=['task1', 'task2', 'task3'])
    # results = [result1, result2, result3]
    
    # Combinar resultados
    total = sum(results)
```

### Pull con default
```python
@task
def consumer(ti):
    # Retorna None si no existe
    data = ti.xcom_pull(task_ids='maybe_task')
    
    # Con default
    data = ti.xcom_pull(task_ids='maybe_task') or {'default': True}
```

### Pull más reciente
```python
@task
def consumer(ti):
    # Sin especificar task_ids, pull del return más reciente
    # (de cualquier tarea upstream)
    data = ti.xcom_pull()
```

## Keys especiales:

### return_value (default key)
```python
@task
def producer():
    return {"data": 100}

@task
def consumer(ti):
    # Ambos son equivalentes:
    data1 = ti.xcom_pull(task_ids='producer')
    data2 = ti.xcom_pull(task_ids='producer', key='return_value')
```

### Keys personalizados
```python
@task
def producer(ti):
    ti.xcom_push(key='config', value={'env': 'prod'})
    ti.xcom_push(key='metrics', value={'count': 100})
    ti.xcom_push(key='errors', value=[])
    return "main result"

@task
def consumer(main_result: str, ti):
    config = ti.xcom_pull(task_ids='producer', key='config')
    metrics = ti.xcom_pull(task_ids='producer', key='metrics')
    errors = ti.xcom_pull(task_ids='producer', key='errors')
```

## Patrón: Return + Metadata

```python
@task
def etl_extract(ti):
    # Resultado principal
    data = {"records": [...], "count": 1000}
    
    # Metadata separado
    ti.xcom_push(key='extraction_metadata', value={
        'source': 'api',
        'timestamp': str(datetime.now()),
        'duration_seconds': 12.5,
        'rows_extracted': 1000
    })
    
    return data

@task
def etl_transform(data: dict, ti):
    # Data del return
    records = data['records']
    
    # Metadata opcional (para logging, monitoring)
    metadata = ti.xcom_pull(task_ids='etl_extract', key='extraction_metadata')
    print(f"Source: {metadata['source']}")
    
    # Transform...
    transformed = [...]
    
    # Push metadata de transform
    ti.xcom_push(key='transform_metadata', value={
        'rows_input': len(records),
        'rows_output': len(transformed),
        'duration_seconds': 8.3
    })
    
    return transformed
```

## Patrón: Fan-out con XCom

```python
@task
def split_work(ti):
    # Return para siguiente tarea
    main_data = {"status": "split complete"}
    
    # Push batches para múltiples workers
    batches = [{'batch': 1, 'data': [...]}, 
               {'batch': 2, 'data': [...]},
               {'batch': 3, 'data': [...]}]
    
    for i, batch in enumerate(batches):
        ti.xcom_push(key=f'batch_{i}', value=batch)
    
    ti.xcom_push(key='batch_count', value=len(batches))
    return main_data

@task
def process_batch(batch_id: int, ti):
    # Pull específico batch
    batch = ti.xcom_pull(task_ids='split_work', key=f'batch_{batch_id}')
    # Process batch...
    return f"Batch {batch_id} processed"
```

## Limitaciones de XCom:

1. **Tamaño**: Limitado por base de datos (típicamente < 1 MB)
2. **Serialización**: Solo objetos serializables (JSON, Pickle)
3. **No para archivos**: Usar storage externo (S3, GCS)
4. **Performance**: No para grandes volúmenes

## Best practices:

1. **Preferir return**: Más limpio y simple
2. **Keys semánticos**: 'config', 'metadata', no 'data1', 'data2'
3. **Documentar keys**: Qué contiene cada key
4. **Datos pequeños**: Solo metadata, configuración
5. **Validar existence**: Check if xcom_pull returns None

## Ejemplo completo:

```python
@task
def extract(ti):
    \"\"\"Extrae datos y guarda metadata\"\"\"
    # Resultado principal
    records = [{"id": 1}, {"id": 2}]
    
    # Metadata en XCom
    ti.xcom_push(key='extract_metadata', value={
        'source': 'api',
        'count': len(records),
        'timestamp': str(datetime.now())
    })
    
    return records

@task
def transform(records: list, ti):
    \"\"\"Transforma datos usando metadata de extract\"\"\"
    # Records del return
    transformed = [{"id": r["id"], "processed": True} for r in records]
    
    # Metadata opcional de extract
    extract_meta = ti.xcom_pull(task_ids='extract', key='extract_metadata')
    print(f"Processing {extract_meta['count']} records from {extract_meta['source']}")
    
    # Metadata de transform
    ti.xcom_push(key='transform_metadata', value={
        'input_count': len(records),
        'output_count': len(transformed)
    })
    
    return transformed

@task
def load(data: list, ti):
    \"\"\"Carga datos y agrega metadata completa\"\"\"
    # Pull all metadata
    extract_meta = ti.xcom_pull(task_ids='extract', key='extract_metadata')
    transform_meta = ti.xcom_pull(task_ids='transform', key='transform_metadata')
    
    # Log completo del pipeline
    print(f"Pipeline complete:")
    print(f"  Extracted {extract_meta['count']} from {extract_meta['source']}")
    print(f"  Transformed {transform_meta['input_count']} -> {transform_meta['output_count']}")
    print(f"  Loading {len(data)} records")
    
    return {"status": "success", "records_loaded": len(data)}

records = extract()
transformed = transform(records)
load(transformed)
```
"""
