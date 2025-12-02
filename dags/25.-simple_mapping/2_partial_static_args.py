"""
Dynamic Task Mapping - partial() para Valores Fijos

partial() permite fijar algunos argumentos mientras
otros son expandidos dinámicamente.
"""

import datetime

from airflow.sdk import DAG, task


@task
def get_files():
    """Retorna lista de archivos a procesar"""
    print("📁 Generando lista de archivos...")
    
    files = [
        's3://bucket/data/file1.csv',
        's3://bucket/data/file2.csv',
        's3://bucket/data/file3.csv',
        's3://bucket/data/file4.csv'
    ]
    
    print(f"✅ {len(files)} archivos para procesar")
    return files


@task
def process_file(file_path: str, chunk_size: int, timeout: int):
    """Procesa un archivo con configuración fija"""
    print(f"📄 Procesando: {file_path}")
    print(f"  - Chunk size: {chunk_size}")
    print(f"  - Timeout: {timeout}s")
    
    # Simular procesamiento
    filename = file_path.split('/')[-1]
    records = 1000  # Simulado
    
    return {
        'file': filename,
        'records': records,
        'chunk_size': chunk_size,
        'status': 'success'
    }


@task
def aggregate_files(results: list):
    """Agrega resultados de todos los archivos"""
    print(f"📊 Agregando {len(results)} archivos...")
    
    total_records = sum(r['records'] for r in results)
    files = [r['file'] for r in results]
    
    print(f"✅ Archivos procesados: {', '.join(files)}")
    print(f"✅ Total records: {total_records}")
    
    return {
        'files_count': len(files),
        'total_records': total_records
    }


with DAG(
    dag_id='dynamic_mapping_partial',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'simple_mapping']
) as dag:
    
    files = get_files()
    
    # partial() fija chunk_size y timeout, expand file_path
    processed = process_file.partial(
        chunk_size=500,
        timeout=300
    ).expand(file_path=files)
    
    summary = aggregate_files(processed)

dag.doc_md = """
# partial() - Valores Fijos + Dynamic Mapping

## Usar partial():
```python
@task
def process(item: str, batch_size: int, timeout: int):
    # Process...

items = get_items()  # Lista dinámica

# batch_size y timeout fijos, item dinámico
process.partial(
    batch_size=1000,
    timeout=300
).expand(item=items)
```

## Cuándo usar partial():
- Configuración común para todas las task instances
- Evitar repetir valores en cada elemento
- Código más limpio y mantenible

## Ejemplo completo:
```python
@task
def process(
    file: str,      # Dinámico (varía)
    format: str,    # Fijo
    compression: str,  # Fijo
    timeout: int    # Fijo
):
    # Process file...

files = get_files()

process.partial(
    format='parquet',
    compression='gzip',
    timeout=600
).expand(file=files)

# Crea N task instances (N = len(files))
# Cada una recibe:
#   file=files[i], format='parquet', 
#   compression='gzip', timeout=600
```

## Ventajas:
- DRY (Don't Repeat Yourself)
- Cambiar config en un solo lugar
- Menos datos pasados por XCom
"""
