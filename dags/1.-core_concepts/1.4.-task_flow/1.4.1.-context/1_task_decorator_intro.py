"""
TaskFlow - Introducción al Decorator @task

Este es el primer ejemplo usando TaskFlow API.
El decorator @task convierte funciones Python en tareas de Airflow.

Más pythónico que usar PythonOperator explícitamente.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator


@task
def extract():
    """Extrae datos de una fuente"""
    print("📥 Extrayendo datos...")
    data = {"users": 100, "orders": 500, "revenue": 25000}
    print(f"✅ Datos extraídos: {data}")
    return data


@task
def transform(data: dict):
    """Transforma los datos extraídos"""
    print(f"⚙️ Transformando datos: {data}")
    transformed = {
        "total_users": data["users"],
        "total_orders": data["orders"],
        "avg_order_value": data["revenue"] / data["orders"]
    }
    print(f"✅ Datos transformados: {transformed}")
    return transformed


@task
def load(data: dict):
    """Carga los datos transformados"""
    print(f"💾 Cargando datos: {data}")
    print(f"  - Total usuarios: {data['total_users']}")
    print(f"  - Total órdenes: {data['total_orders']}")
    print(f"  - Valor promedio: ${data['avg_order_value']:.2f}")
    print("✅ Datos cargados exitosamente")


with DAG(
    dag_id='taskflow_basic_decorator',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'task_flow', 'context']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Invocar tareas decoradas
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
    
    end = EmptyOperator(task_id='end')
    
    # Dependencias
    start >> data >> end

dag.doc_md = """
# TaskFlow API - @task Decorator

**TaskFlow API**: Introducida en Airflow 2.0, simplifica la creación de tareas Python.

## Antes (PythonOperator):

```python
def extract_func():
    data = {"users": 100}
    return data

def transform_func(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract')
    transformed = {"total": data["users"]}
    return transformed

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_func,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_func,
    dag=dag
)

extract_task >> transform_task
```

## Ahora (TaskFlow):

```python
@task
def extract():
    data = {"users": 100}
    return data

@task
def transform(data: dict):
    transformed = {"total": data["users"]}
    return transformed

data = extract()
transformed = transform(data)
```

## Ventajas:

1. **Menos código**: No necesita PythonOperator explícito
2. **Datos implícitos**: Return pasa datos automáticamente (XCom)
3. **Type hints**: Mejor autocompletado y documentación
4. **Pythónico**: Siente como Python normal
5. **Dependencias claras**: data = extract() establece dependencia

## Cómo funciona:

```python
@task
def my_task():
    return "result"

# Airflow internamente:
# 1. Crea PythonOperator
# 2. task_id = nombre de función ('my_task')
# 3. python_callable = my_task
# 4. XCom automático para return value
```

## Task ID:

Por defecto, task_id = nombre de función:

```python
@task
def extract():  # task_id='extract'
    pass

@task(task_id='custom_name')
def extract():  # task_id='custom_name'
    pass
```

## Paso de datos:

```python
@task
def task_a():
    return {"value": 100}

@task
def task_b(data: dict):  # Recibe return de task_a
    print(data)  # {"value": 100}
    return data["value"] * 2

@task
def task_c(number: int):  # Recibe return de task_b
    print(number)  # 200

a = task_a()
b = task_b(a)
c = task_c(b)

# Dependencias implícitas: task_a >> task_b >> task_c
```

## Múltiples argumentos:

```python
@task
def task_a():
    return 10

@task
def task_b():
    return 20

@task
def task_c(x: int, y: int):
    return x + y

a = task_a()
b = task_b()
c = task_c(a, b)  # task_c depende de task_a y task_b

# Dependencias: [task_a, task_b] >> task_c
```

## Parámetros adicionales:

```python
@task(
    task_id='custom_id',
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=30)
)
def my_task():
    pass
```

## Compatibilidad con operadores tradicionales:

```python
from airflow.providers.standard.operators.bash import BashOperator

@task
def python_task():
    return "data"

bash_task = BashOperator(
    task_id='bash',
    bash_command='echo "hello"'
)

data = python_task()
data >> bash_task  # TaskFlow + operador tradicional
```

## Cuándo usar TaskFlow:

✅ **Usar TaskFlow cuando**:
- Lógica en Python
- Necesitas pasar datos entre tareas
- Código limpio y mantenible

❌ **No usar TaskFlow cuando**:
- Operadores específicos (S3, Snowflake, etc.)
- Bash scripts simples
- Necesitas features de operadores específicos

## Type hints (recomendado):

```python
@task
def process(data: dict) -> dict:
    # Editor sabe que data es dict
    # Mejor autocompletado
    return {"processed": data}
```

## Debugging:

```python
@task
def debug_task(data: dict):
    print(f"Received: {data}")  # Ver en logs
    print(f"Type: {type(data)}")
    return data
```
"""
