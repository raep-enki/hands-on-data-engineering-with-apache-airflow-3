"""
Challenge: Procesar N Fuentes Sin Saber Cuántas Son

Tienes un pipeline que extrae de S3, GCS, Azure Blob, y FTP. Mañana agregan SFTP y HTTP. Pasado
agregan 3 buckets S3 más. Podrías crear una tarea por fuente (extract_s3, extract_gcs, etc.),
pero cada vez que cambia la lista, debes modificar el DAG. No escala.

La solución: **Dynamic Task Mapping** con `.expand()`. Una tarea genera la lista de fuentes
(puede ser dinámica, de DB, de API, etc.), y otra tarea se "mapea" sobre esa lista, creando
una instancia paralela por cada elemento. Es como `for source in sources:` pero cada iteración
es una tarea independiente que corre en paralelo.

**IMPORTANTE: Todas las tareas usan PythonOperator (@task)** porque necesitan:
- Generar listas dinámicas de fuentes (lógica Python)
- Extraer de APIs/cloud storage (librerías Python: boto3, google-cloud-storage)
- Validar datos complejos (pandas, custom logic)
- Transformar y consolidar resultados
- .expand() requiere @task (no funciona con BashOperator fácilmente)

BashOperator no maneja listas dinámicas bien. Usa @task para dynamic mapping.

**El pipeline con dynamic mapping:**

**Task 1: Generar lista de fuentes (PythonOperator - puede ser dinámico)**
```python
@task(task_id='list_data_sources')
def get_sources():
    # En producción: consulta DB, API, o archivo config
    sources = [
        {'type': 's3', 'bucket': 'data-lake-raw', 'prefix': 'sales/'},
        {'type': 's3', 'bucket': 'data-lake-raw', 'prefix': 'marketing/'},
        {'type': 'gcs', 'bucket': 'analytics-data', 'prefix': 'events/'},
        {'type': 'azure', 'container': 'logs', 'prefix': 'app-logs/'},
        {'type': 'ftp', 'host': 'ftp.partner.com', 'path': '/data/'},
        {'type': 'sftp', 'host': 'sftp.vendor.com', 'path': '/exports/'}
    ]
    print(f"Found {len(sources)} data sources to process")
    return sources  # Lista que se usa para .expand()
```

**Task 2: Extraer de cada fuente (SE MAPEA - corre en paralelo por cada fuente)**
```python
@task(task_id='extract_from_source')
def extract(source):
    # Esta función corre UNA VEZ por cada elemento en sources
    # Si hay 6 fuentes, crea 6 instancias de esta tarea en paralelo
    source_type = source['type']
    
    if source_type == 's3':
        print(f"Extracting from S3: s3://{source['bucket']}/{source['prefix']}")
        # En producción: boto3.client('s3').list_objects_v2(...)
        records = 1500
    elif source_type == 'gcs':
        print(f"Extracting from GCS: gs://{source['bucket']}/{source['prefix']}")
        # En producción: storage.Client().bucket(...).list_blobs(...)
        records = 2300
    elif source_type == 'azure':
        print(f"Extracting from Azure: {source['container']}/{source['prefix']}")
        records = 1800
    elif source_type == 'ftp':
        print(f"Extracting from FTP: {source['host']}{source['path']}")
        records = 950
    elif source_type == 'sftp':
        print(f"Extracting from SFTP: {source['host']}{source['path']}")
        records = 1200
    
    return {'source': source, 'record_count': records}
```

**Task 3: Validar cada extracción (SE MAPEA - una por cada extract)**
```python
@task(task_id='validate_extraction')
def validate(extraction_result):
    # Corre una vez por cada resultado de extract
    source = extraction_result['source']
    count = extraction_result['record_count']
    
    print(f"Validating {count} records from {source['type']}")
    
    # Validaciones
    is_valid = count > 0 and count < 1_000_000
    
    if is_valid:
        print(f"✓ Validation passed for {source['type']}")
        return {'source': source, 'count': count, 'valid': True}
    else:
        print(f"✗ Validation failed for {source['type']}")
        return {'source': source, 'count': count, 'valid': False}
```

**Task 4: Consolidar todos los resultados (NO SE MAPEA - recibe lista completa)**
```python
@task(task_id='consolidate_all_sources')
def consolidate(validation_results):
    # Recibe LISTA con resultados de todas las validaciones
    print(f"Consolidating {len(validation_results)} sources")
    
    total_records = sum(r['count'] for r in validation_results)
    valid_sources = [r for r in validation_results if r['valid']]
    invalid_sources = [r for r in validation_results if not r['valid']]
    
    print(f"Total records: {total_records:,}")
    print(f"Valid sources: {len(valid_sources)}/{len(validation_results)}")
    
    if invalid_sources:
        print(f"⚠ Warning: {len(invalid_sources)} sources failed validation")
    
    return {
        'total_records': total_records,
        'valid_count': len(valid_sources),
        'invalid_count': len(invalid_sources)
    }
```

**Task 5: Generar reporte (PythonOperator - lógica de reporting)**
```python
@task(task_id='generate_ingestion_report')
def report(consolidation):
    print("INGESTION REPORT")
    print("================")
    print(f"Total Records Ingested: {consolidation['total_records']:,}")
    print(f"Valid Sources: {consolidation['valid_count']}")
    print(f"Failed Sources: {consolidation['invalid_count']}")
    
    success_rate = consolidation['valid_count'] / (consolidation['valid_count'] + consolidation['invalid_count'])
    print(f"Success Rate: {success_rate:.1%}")
    
    return "Report generated"
```

**Flujo con .expand():**

```python
with DAG(
    dag_id='simple_mapping_challenge',
    schedule='@daily',
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=['challenge', 'simple_mapping']
) as dag:
    sources = get_sources()
    
    # .expand() mapea extract sobre cada elemento de sources
    # Crea N tareas en paralelo (N = len(sources))
    extracts = extract.expand(source=sources)
    
    # .expand() mapea validate sobre cada resultado de extract
    # También corre en paralelo, una por cada extract
    validations = validate.expand(extraction_result=extracts)
    
    # consolidate NO usa .expand(), recibe lista completa
    consolidated = consolidate(validations)
    
    # reporte final
    final_report = report(consolidated)
```

**Visual del grafo:**
```
get_sources → [extract[0], extract[1], extract[2], ...] → 
              [validate[0], validate[1], validate[2], ...] →
              consolidate → report
```

**Ventajas de dynamic mapping:**
- **Escalabilidad:** 4 fuentes hoy, 50 mañana, mismo código
- **Paralelismo:** Todas las extracciones corren simultáneamente
- **Flexibilidad:** Lista de fuentes puede venir de DB, API, config
- **Claridad:** Código más limpio que crear tareas manualmente

**Cuándo usar .expand():**
- Procesamiento de múltiples archivos/tablas/APIs
- Fan-out/fan-in patterns
- Listas dinámicas (no conocidas en DAG authoring time)
- Operaciones idénticas sobre N elementos

**Configuración técnica:**
- DAG ID: `simple_mapping_challenge`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'simple_mapping']`
- 5 funciones @task: 1 genera lista, 2 se mapean, 2 consolidan
- .expand() en 2 tareas (extract y validate)
- Usa PythonOperator (@task) para TODAS las tareas (dynamic mapping)
"""

import datetime

from airflow.sdk import DAG, task

# TODO: Usa @task con .expand() para dynamic task mapping
