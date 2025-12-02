"""
TaskFlow - Serializar Objetos No-JSON

Demuestra cómo manejar objetos que no son directamente
serializables a JSON (datetime, Pandas, custom classes).

Solución: Convertir a tipos JSON-serializables.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG, task


@task
def extract_with_timestamp() -> dict:
    """Retorna datos con timestamp como string"""
    print("📅 Extrayendo con timestamp...")
    
    # datetime → ISO string
    now = datetime.datetime.now()
    
    data = {
        'timestamp': now.isoformat(),  # Serializable
        'date': now.strftime('%Y-%m-%d'),
        'records': [
            {'id': 1, 'created_at': (now - timedelta(hours=2)).isoformat()},
            {'id': 2, 'created_at': (now - timedelta(hours=1)).isoformat()}
        ]
    }
    
    print(f"✅ Timestamp: {data['timestamp']}")
    return data


@task
def process_timestamps(data: dict) -> dict:
    """Procesa strings de timestamp como datetime"""
    print("⚙️ Procesando timestamps...")
    
    # String → datetime
    timestamp = datetime.datetime.fromisoformat(data['timestamp'])
    
    for record in data['records']:
        created = datetime.datetime.fromisoformat(record['created_at'])
        age_hours = (timestamp - created).total_seconds() / 3600
        record['age_hours'] = round(age_hours, 1)
    
    print(f"✅ Procesados {len(data['records'])} records")
    return data


@task
def simulate_pandas() -> dict:
    """Simula DataFrame de Pandas serializado"""
    print("🐼 Simulando Pandas DataFrame...")
    
    # En realidad sería:
    # df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    # return {'data': df.to_dict('records')}
    
    df_data = [
        {'name': 'Alice', 'age': 30, 'score': 95},
        {'name': 'Bob', 'age': 25, 'score': 87},
        {'name': 'Charlie', 'age': 35, 'score': 92}
    ]
    
    return {
        'data': df_data,
        'columns': ['name', 'age', 'score'],
        'shape': (3, 3)
    }


@task
def process_dataframe(df_dict: dict) -> dict:
    """Procesa dict que representa DataFrame"""
    print("⚙️ Procesando DataFrame...")
    
    data = df_dict['data']
    
    # Calcular stats
    avg_age = sum(row['age'] for row in data) / len(data)
    avg_score = sum(row['score'] for row in data) / len(data)
    
    return {
        'rows': len(data),
        'avg_age': round(avg_age, 1),
        'avg_score': round(avg_score, 1),
        'data': data
    }


with DAG(
    dag_id='taskflow_serialize_objects',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'task_flow', 'passing_arbitrary_objects_as_arguments']
) as dag:
    
    # Pipeline con objetos serializados
    timestamped = extract_with_timestamp()
    processed = process_timestamps(timestamped)
    
    df = simulate_pandas()
    stats = process_dataframe(df)

dag.doc_md = """
# Serializar Objetos No-JSON

## Datetime objects:
```python
@task
def with_datetime():
    now = datetime.now()
    return {'timestamp': now.isoformat()}  # ✅

@task
def use_datetime(data: dict):
    ts = datetime.fromisoformat(data['timestamp'])
```

## Pandas DataFrames:
```python
@task
def with_pandas():
    df = pd.DataFrame({'a': [1, 2]})
    return {'data': df.to_dict('records')}  # ✅

@task
def use_pandas(data: dict):
    df = pd.DataFrame(data['data'])
```

## Datos grandes → Storage externo:
```python
@task
def large_data():
    df = generate_large_df()
    path = upload_to_s3(df)
    return {'s3_path': path}  # ✅ Solo referencia

@task
def process_large(meta: dict):
    df = download_from_s3(meta['s3_path'])
```
"""
