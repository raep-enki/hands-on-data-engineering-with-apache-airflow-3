"""
TaskFlow - Validación y Transformación de Tipos

Demuestra buenas prácticas para validar y transformar
datos pasados entre tareas TaskFlow.

Importante para robustez y debugging.
"""

import datetime

from airflow.sdk import DAG, task


@task
def extract_raw_data() -> dict:
    """Extrae datos sin validación"""
    return {
        'users': [
            {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'age': '30'},
            {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'age': '25'},
            {'id': '3', 'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35}
        ],
        'count': '3',
        'extracted_at': datetime.datetime.now().isoformat()
    }


@task
def validate_and_clean(data: dict) -> dict:
    """Valida tipos y limpia datos"""
    print("🔍 Validando y limpiando datos...")
    
    # Validar estructura
    assert 'users' in data, "Missing 'users' key"
    assert isinstance(data['users'], list), "'users' must be list"
    
    # Limpiar tipos
    cleaned_users = []
    for user in data['users']:
        cleaned_users.append({
            'id': int(user['id']),  # Asegurar int
            'name': str(user['name']),
            'email': str(user['email']),
            'age': int(user['age'])  # Convertir string a int
        })
    
    return {
        'users': cleaned_users,
        'count': int(data['count']),
        'extracted_at': data['extracted_at']
    }


@task
def transform_with_validation(data: dict) -> dict:
    """Transforma con validación de business rules"""
    print("⚙️ Transformando con validación...")
    
    transformed = []
    errors = []
    
    for user in data['users']:
        # Business rules
        if user['age'] < 18:
            errors.append(f"User {user['id']}: age below 18")
            continue
        
        if '@' not in user['email']:
            errors.append(f"User {user['id']}: invalid email")
            continue
        
        # Transform
        transformed.append({
            **user,
            'category': 'senior' if user['age'] >= 30 else 'junior',
            'valid': True
        })
    
    if errors:
        print(f"⚠️ Errors found: {errors}")
    
    return {
        'users': transformed,
        'valid_count': len(transformed),
        'error_count': len(errors),
        'errors': errors
    }


@task
def assert_data_quality(data: dict) -> dict:
    """Assert data quality antes de load"""
    print("✅ Verificando calidad de datos...")
    
    # Assertions
    assert data['valid_count'] > 0, "No valid records"
    assert data['error_count'] < data['valid_count'], "Too many errors"
    
    for user in data['users']:
        assert user['valid'], f"User {user['id']} not valid"
        assert user['age'] >= 18, f"User {user['id']} underage"
    
    print(f"✅ Quality OK: {data['valid_count']} valid, {data['error_count']} errors")
    return data


with DAG(
    dag_id='taskflow_validation',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'passing_arbitrary_objects_as_arguments']
) as dag:
    
    raw = extract_raw_data()
    cleaned = validate_and_clean(raw)
    transformed = transform_with_validation(cleaned)
    validated = assert_data_quality(transformed)

dag.doc_md = """
# Validación y Transformación

## Pattern de validación:
1. **Extract**: Datos crudos
2. **Validate**: Tipos correctos
3. **Transform**: Business logic
4. **Assert**: Quality gates

## Type conversion:
```python
cleaned = {
    'id': int(raw['id']),
    'count': int(raw['count']),
    'price': float(raw['price'])
}
```

## Assertions:
```python
assert len(data) > 0, "No data"
assert all(r['valid'] for r in data), "Invalid records"
```
"""
