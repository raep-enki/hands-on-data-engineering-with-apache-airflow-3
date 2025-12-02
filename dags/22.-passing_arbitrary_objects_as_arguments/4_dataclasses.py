"""
TaskFlow - Pasar Objetos Estructurados entre Tareas

Demuestra cómo TaskFlow API pasa automáticamente
objetos estructurados (dicts) entre tareas. 

NOTA: Airflow 3.x tiene un bug conocido con @dataclass en template rendering.
Como workaround, usamos dicts estructurados con type hints.
"""

import datetime

from airflow.sdk import DAG, task


@task
def fetch_user() -> dict:
    """Retorna un dict estructurado como UserProfile"""
    print("📥 Fetching user...")
    
    user = {
        'user_id': 123,
        'name': "Alice Johnson",
        'email': "alice@example.com",
        'age': 32
    }
    
    print(f"✅ Fetched: {user['name']}")
    return user


@task
def process_user(profile: dict) -> dict:
    """Recibe profile dict, retorna processed dict"""
    print(f"⚙️ Processing user: {profile['name']}")
    
    # Clasificar por edad
    category = 'senior' if profile['age'] >= 30 else 'junior'
    
    # Calcular score (ejemplo simple)
    risk_score = (profile['age'] * 0.1) + 5.0
    
    processed = {
        'profile': profile,
        'category': category,
        'risk_score': risk_score
    }
    
    print(f"✅ Processed: category={category}, score={risk_score}")
    return processed


@task
def save_user(user: dict) -> dict:
    """Recibe processed dict y guarda"""
    profile = user['profile']
    print(f"💾 Saving user: {profile['name']}")
    print(f"  - Category: {user['category']}")
    print(f"  - Risk Score: {user['risk_score']}")
    print(f"  - Email: {profile['email']}")
    
    return {
        'status': 'saved',
        'user_id': profile['user_id'],
        'category': user['category']
    }


with DAG(
    dag_id='taskflow_structured_dicts',
    schedule='@daily',
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'passing_arbitrary_objects_as_arguments']
) as dag:
    
    # Pipeline: dicts estructurados pasan automáticamente
    user_profile = fetch_user()
    processed = process_user(user_profile)
    result = save_user(processed)


dag.doc_md = """
# Pasar Objetos Estructurados entre Tareas

TaskFlow API soporta pasar dicts estructurados entre tareas usando XCom.

## Workaround para Airflow 3.x:

Airflow 3.x tiene un bug con @dataclass en template rendering. Solución:

```python
# ✓ FUNCIONA: Dict estructurado con type hints
@task
def create() -> dict:
    return {'value': 42, 'name': "test"}

@task  
def use(data: dict):
    print(data['value'])  # 42
```

## Cuando se resuelva el bug, podrás usar:

```python
from dataclasses import dataclass

@dataclass
class MyData:
    value: int
    name: str

@task
def create() -> MyData:
    return MyData(value=42, name="test")

@task  
def use(data: MyData):
    print(data.value)  # 42
```

## Referencias:

- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html#passing-arbitrary-objects-as-arguments)
- Dataclass support desde Airflow 2.5.0
"""
