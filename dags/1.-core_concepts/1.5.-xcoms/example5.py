"""
XComs - Limitaciones y Best Practices

Demuestra limitaciones de XCom y cómo manejarlas.
XCom NO es para datos grandes - usar storage externo.
"""

import datetime
import json

from airflow.sdk import DAG, task


@task
def good_xcom_usage():
    """✅ Uso correcto: datos pequeños"""
    print("✅ XCom apropiado: metadata pequeña")
    
    # Pequeños metadata, configuración, referencias
    return {
        'file_path': 's3://bucket/data/file.csv',
        'record_count': 1000,
        'status': 'success',
        'checksum': 'abc123',
        'size_mb': 5.2
    }


@task
def bad_xcom_usage():
    """❌ Mal uso: intentar pasar datos grandes"""
    print("⚠️ XCom inapropiado: datos grandes")
    
    # ❌ NO hacer esto - lista muy grande
    # large_data = [{'id': i, 'data': 'x'*1000} for i in range(10000)]
    # return large_data  # Esto puede fallar o ser muy lento
    
    # ✅ En su lugar: guardar en storage y pasar referencia
    print("  - Guardando datos grandes en S3...")
    # save_to_s3(large_data, 's3://bucket/data.parquet')
    
    return {
        's3_path': 's3://bucket/data.parquet',
        'record_count': 10000,
        'format': 'parquet'
    }


@task
def xcom_size_check(ti):
    """Verificar tamaño de XComs"""
    print("📊 Verificando tamaño de XComs...")
    
    good_data = ti.xcom_pull(task_ids='good_xcom_usage')
    bad_data = ti.xcom_pull(task_ids='bad_xcom_usage')
    
    good_size = len(json.dumps(good_data))
    bad_size = len(json.dumps(bad_data))
    
    print(f"  - good_xcom_usage: {good_size} bytes ✅")
    print(f"  - bad_xcom_usage: {bad_size} bytes ✅ (solo referencia)")
    
    # Límites típicos:
    # - PostgreSQL: 1 GB (pero no recomendado)
    # - MySQL: 64 KB default (MEDIUMTEXT: 16 MB)
    # - SQLite: limitado por memoria
    
    return {
        'good_size_bytes': good_size,
        'bad_size_bytes': bad_size,
        'recommendation': 'Use external storage for > 1 MB'
    }


@task
def handle_large_data():
    """✅ Pattern correcto para datos grandes"""
    print("📦 Manejo correcto de datos grandes...")
    
    # 1. Generar/procesar datos grandes
    # large_dataset = process_large_file()
    
    # 2. Guardar en storage externo
    print("  - Guardando en S3...")
    # s3_path = upload_to_s3(large_dataset, bucket='data', key='processed/data.parquet')
    s3_path = 's3://data/processed/data.parquet'
    
    # 3. Solo pasar metadata por XCom
    return {
        's3_path': s3_path,
        'record_count': 1000000,
        'size_mb': 250.5,
        'format': 'parquet',
        'partitions': ['year=2024', 'month=01']
    }


@task
def use_large_data(metadata: dict):
    """Usa datos grandes desde storage externo"""
    print("📥 Descargando datos desde storage...")
    
    s3_path = metadata['s3_path']
    print(f"  - Path: {s3_path}")
    print(f"  - Records: {metadata['record_count']}")
    
    # Descargar y procesar
    # data = download_from_s3(s3_path)
    # process(data)
    
    print("✅ Datos procesados")
    return {'status': 'processed', 'source': s3_path}


with DAG(
    dag_id='xcoms_best_practices',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'xcoms']
) as dag:
    
    # Comparación de usos
    good = good_xcom_usage()
    bad = bad_xcom_usage()
    check = xcom_size_check()
    
    # Pattern correcto
    metadata = handle_large_data()
    processed = use_large_data(metadata)
    
    [good, bad] >> check
    metadata >> processed

dag.doc_md = """
# XCom Best Practices

## ✅ Usar XCom para:
- **Metadata**: paths, counts, status
- **Configuración**: settings, parameters
- **Referencias**: IDs, keys, URLs
- **Stats pequeños**: counts, sums, averages
- **Control flow**: flags, conditions

## ❌ NO usar XCom para:
- **Datos grandes**: > 1 MB
- **Archivos**: CSVs, Parquet, JSON grandes
- **DataFrames**: Pandas, Spark DataFrames
- **Imágenes**: Binarios grandes
- **Logs**: Output extenso

## Pattern para datos grandes:

```python
@task
def process_large():
    # Procesar datos
    df = process_data()
    
    # Guardar en storage externo
    s3_path = upload_to_s3(df, 'bucket/key')
    
    # Solo metadata por XCom
    return {
        's3_path': s3_path,
        'record_count': len(df),
        'size_mb': df.memory_usage().sum() / 1024**2
    }

@task
def use_large(metadata: dict):
    # Descargar desde storage
    df = download_from_s3(metadata['s3_path'])
    # Procesar...
```

## Límites de tamaño:
- PostgreSQL: ~1 GB (no recomendado)
- MySQL: 64 KB - 16 MB (según config)
- SQLite: Limitado por memoria
- **Recomendado**: < 1 MB por XCom

## Alternativas para datos grandes:
1. **S3/GCS**: Archivos en cloud storage
2. **HDFS**: Hadoop filesystem
3. **Shared filesystem**: NFS, EFS
4. **Database tables**: Staging tables
5. **Object storage**: MinIO, Ceph

## Monitoring XCom size:
```python
# Ver XComs grandes en DB
SELECT task_id, key, LENGTH(value) as size_bytes
FROM xcom
WHERE dag_id = 'my_dag'
ORDER BY size_bytes DESC
LIMIT 10;
```

## Cleanup:
XComs se limpian automáticamente según:
- `dag.max_active_runs_per_dag`
- Airflow config: `xcom_backend`
- Manual: `airflow db clean`
"""
