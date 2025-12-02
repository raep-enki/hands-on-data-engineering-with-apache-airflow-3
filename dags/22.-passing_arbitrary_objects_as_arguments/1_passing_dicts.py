"""
TaskFlow - Pasar Objetos Arbitrarios (Dicts)

Demuestra cómo TaskFlow API pasa automáticamente
objetos Python entre tareas usando XCom.

Los returns se serializan y pasan a la siguiente tarea.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator


@task
def extract_users():
    """Extrae datos de usuarios como dict"""
    print("👥 Extrayendo usuarios...")
    
    users = {
        'total': 3,
        'data': [
            {'id': 1, 'name': 'Alice', 'age': 30, 'city': 'NYC'},
            {'id': 2, 'name': 'Bob', 'age': 25, 'city': 'SF'},
            {'id': 3, 'name': 'Charlie', 'age': 35, 'city': 'LA'}
        ]
    }
    
    print(f"✅ Extraídos {users['total']} usuarios")
    return users


@task
def transform_users(users: dict):
    """Transforma dict de usuarios"""
    print(f"⚙️ Transformando {users['total']} usuarios...")
    
    # Agregar campo calculado
    for user in users['data']:
        user['category'] = 'senior' if user['age'] >= 30 else 'junior'
        user['name_upper'] = user['name'].upper()
    
    transformed = {
        'total': users['total'],
        'data': users['data'],
        'transformation': 'added category and name_upper'
    }
    
    print(f"✅ Transformación completa")
    return transformed


@task
def load_users(users: dict):
    """Carga dict de usuarios"""
    print(f"💾 Cargando {users['total']} usuarios...")
    
    for user in users['data']:
        print(f"  - {user['name_upper']}: {user['age']} years, {user['category']}, {user['city']}")
    
    print(f"✅ Carga completa")
    return {'status': 'success', 'loaded': users['total']}


with DAG(
    dag_id='taskflow_pass_dicts',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'passing_arbitrary_objects_as_arguments']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Pipeline: dict pasa automáticamente entre tareas
    users = extract_users()
    transformed = transform_users(users)
    result = load_users(transformed)
    
    end = EmptyOperator(task_id='end')
    
    start >> users >> end

dag.doc_md = """
# Pasar Objetos entre Tareas

TaskFlow API serializa automáticamente returns y los pasa como argumentos.

## Serialización automática:

```python
@task
def task_a():
    data = {"key": "value", "number": 123}
    return data  # Serializado a XCom

@task
def task_b(data: dict):  # Deserializado desde XCom
    print(data)  # {"key": "value", "number": 123}

data = task_a()
task_b(data)
```

## Tipos soportados:

### Tipos básicos:
- **int, float, str, bool**: Directamente serializables
- **None**: También soportado

### Colecciones:
- **list**: `[1, 2, 3]`
- **dict**: `{"key": "value"}`
- **tuple**: `(1, 2, 3)` (se convierte a list)
- **set**: `{1, 2, 3}` (se convierte a list)

### Objetos nested:
```python
@task
def complex_structure():
    return {
        'users': [
            {'id': 1, 'name': 'Alice', 'tags': ['admin', 'user']},
            {'id': 2, 'name': 'Bob', 'tags': ['user']}
        ],
        'metadata': {
            'count': 2,
            'extracted_at': '2024-01-15'
        }
    }

@task
def process(data: dict):
    users = data['users']
    metadata = data['metadata']
    # Trabajar con estructura nested
```

## Type hints (recomendado):

```python
@task
def extract() -> dict:
    return {"data": [1, 2, 3]}

@task
def transform(data: dict) -> list:
    return data["data"]

@task
def load(items: list) -> dict:
    return {"status": "success", "count": len(items)}
```

## Serialización bajo el capó:

```python
# Airflow internamente hace:
# 1. task_a returns data
# 2. json.dumps(data) → XCom
# 3. task_b lee XCom
# 4. json.loads(xcom_value) → argumento de task_b
```

## Limitaciones:

### ❌ No soportado directamente:
- **datetime objects**: Convertir a string
- **Pandas DataFrames**: Serializar a dict/list
- **Custom classes**: No serializables por JSON
- **Functions/lambdas**: No serializables

### ✅ Soluciones:

```python
# Datetime → string
@task
def task_with_date():
    from datetime import datetime
    now = datetime.now()
    return {'timestamp': now.isoformat()}  # String

@task
def use_date(data: dict):
    from datetime import datetime
    timestamp = datetime.fromisoformat(data['timestamp'])

# Pandas → dict
@task
def task_with_pandas():
    import pandas as pd
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    return df.to_dict('records')  # Lista de dicts

@task
def use_pandas(records: list):
    import pandas as pd
    df = pd.DataFrame(records)
```

## Tamaño de datos:

XCom tiene límites de tamaño (típicamente 1-64 MB según base de datos).

### Para datos grandes:
```python
@task
def large_data():
    # Guardar en S3/GCS
    data = generate_large_dataset()
    s3_path = upload_to_s3(data, 'bucket/key')
    return {'s3_path': s3_path}  # Solo referencia

@task
def process_large(metadata: dict):
    # Descargar desde S3
    s3_path = metadata['s3_path']
    data = download_from_s3(s3_path)
    # Procesar...
```

## Múltiples valores:

```python
@task
def multiple_outputs():
    users = [...]
    orders = [...]
    
    # Retornar dict con múltiples valores
    return {
        'users': users,
        'orders': orders,
        'metadata': {'count_users': len(users), 'count_orders': len(orders)}
    }

@task
def process_multiple(data: dict):
    users = data['users']
    orders = data['orders']
    metadata = data['metadata']
```

## Best practices:

1. **Type hints**: Siempre usar para claridad
2. **Datos pequeños**: Solo metadata, no archivos grandes
3. **Serialización explícita**: datetime → string
4. **Validación**: Check tipo/estructura en task receptora
5. **Documentación**: Documentar estructura de datos

## Ejemplo completo:

```python
@task
def extract() -> dict:
    \"\"\"Extrae y retorna estructura compleja\"\"\"
    return {
        'users': [
            {'id': 1, 'name': 'Alice', 'orders': [101, 102]},
            {'id': 2, 'name': 'Bob', 'orders': [103]}
        ],
        'metadata': {
            'extracted_at': datetime.now().isoformat(),
            'source': 'api',
            'count': 2
        }
    }

@task
def validate(data: dict) -> dict:
    \"\"\"Valida estructura\"\"\"
    assert 'users' in data, "Missing users"
    assert 'metadata' in data, "Missing metadata"
    assert isinstance(data['users'], list), "Users must be list"
    
    print(f"Validated {len(data['users'])} users")
    return data

@task
def transform(data: dict) -> dict:
    \"\"\"Transforma datos\"\"\"
    users = data['users']
    
    # Agregar campo calculado
    for user in users:
        user['order_count'] = len(user['orders'])
    
    return {
        'users': users,
        'transformation': 'added order_count'
    }

@task
def load(data: dict) -> dict:
    \"\"\"Carga datos\"\"\"
    users = data['users']
    
    for user in users:
        print(f"{user['name']}: {user['order_count']} orders")
    
    return {'status': 'success', 'loaded': len(users)}

# Pipeline
data = extract()
validated = validate(data)
transformed = transform(validated)
result = load(transformed)
```
"""
