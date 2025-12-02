"""
Challenge: Tareas Que Necesitan Compartir Información

Tienes un pipeline donde una tarea calcula métricas (total de ventas, número de clientes, revenue)
y otras tres tareas necesitan esos números para generar reportes diferentes. Una tarea hace cálculos
complejos (average order value, revenue per customer) y varias tareas downstream los usan.

El problema: ¿cómo pasa datos una tarea a otra? **XComs** (Cross-Communications) es la respuesta:
una tarea "push" datos con un nombre (key), y otras tareas hacen "pull" usando ese nombre.
Es como variables compartidas entre tareas.

**Con TaskFlow API (@task), XCom es automático:** el return de una función se pushea, y cuando
pasas el resultado como argumento, se pullea automáticamente.

**El flujo con XComs:**

**Task 1: Calcular métricas base**
```python
@task(task_id='calculate_base_metrics')
def calculate_metrics():
    # Simula cálculos de ventas
    total_sales = 1_500_000
    num_customers = 3_200
    num_orders = 8_500
    
    print(f"Calculated: ${total_sales} from {num_customers} customers, {num_orders} orders")
    
    # TaskFlow automáticamente pushea este dict a XCom
    return {
        'total_sales': total_sales,
        'num_customers': num_customers,
        'num_orders': num_orders
    }
```

**Task 2: Calcular métricas derivadas (usa XCom pull)**
```python
@task(task_id='calculate_derived_metrics')
def calculate_derived(base_metrics):
    # TaskFlow automáticamente pullea base_metrics del XCom anterior
    aov = base_metrics['total_sales'] / base_metrics['num_orders']
    rpc = base_metrics['total_sales'] / base_metrics['num_customers']
    orders_per_customer = base_metrics['num_orders'] / base_metrics['num_customers']
    
    print(f"Average Order Value: ${aov:.2f}")
    print(f"Revenue Per Customer: ${rpc:.2f}")
    print(f"Orders Per Customer: {orders_per_customer:.2f}")
    
    # Retorna otro dict que también se pushea a XCom
    return {
        'aov': aov,
        'rpc': rpc,
        'orders_per_customer': orders_per_customer
    }
```

**Task 3: Generar reporte ejecutivo (usa ambos XComs)**
```python
@task(task_id='generate_executive_report')
def generate_exec_report(base_metrics, derived_metrics):
    # Usa datos de 2 tareas diferentes
    report = f""""""
    print(report)
    return report
```

**Task 4: Generar reporte de marketing (solo usa base)**
```python
@task(task_id='generate_marketing_report')
def generate_marketing_report(base_metrics):
    # Solo necesita métricas base
    report = f""""""
    print(report)
    return report
```

**Task 5: Generar reporte financiero (usa todos)**
```python
@task(task_id='generate_financial_report')
def generate_financial_report(base_metrics, derived_metrics):
    # Usa toda la información disponible
    report = f""""""
    print(report)
    return report
```

**Task 6: XCom push/pull explícito (usando context)**
```python
@task(task_id='manual_xcom_example')
def manual_xcom_push_pull(**context):
    # Método explícito de XCom (cuando necesitas control fino)
    ti = context['task_instance']
    
    # Pull de otra tarea (alternativa a parámetros)
    base = ti.xcom_pull(task_ids='calculate_base_metrics')
    derived = ti.xcom_pull(task_ids='calculate_derived_metrics')
    
    print(f"Pulled from XCom: Sales=${base['total_sales']}, AOV=${derived['aov']:.2f}")
    
    # Push con key custom (no solo 'return_value')
    ti.xcom_push(key='combined_analysis', value={
        'total_revenue': base['total_sales'],
        'efficiency_score': derived['aov'] / 100
    })
    
    # Push múltiples keys
    ti.xcom_push(key='alert_threshold', value=1000)
    ti.xcom_push(key='report_email', value='exec@company.com')
    
    return "Manual XCom operations completed"
```

**Flujo completo con dependencias:**

```python
with DAG(
    dag_id='xcoms_challenge',
    schedule='@daily',
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=['challenge', 'xcoms']
) as dag:
    # Calcular métricas base
    base = calculate_metrics()
    
    # Calcular métricas derivadas (depende de base)
    derived = calculate_derived(base)
    
    # Generar reportes en paralelo (todos usan base y/o derived)
    exec_report = generate_exec_report(base, derived)
    marketing_report = generate_marketing_report(base)
    financial_report = generate_financial_report(base, derived)
    
    # Ejemplo manual de XCom
    manual = manual_xcom_example()
    
    # Dependencies: base debe terminar antes de derived y reportes
    base >> derived
    [derived, base] >> [exec_report, marketing_report, financial_report]
    [exec_report, marketing_report, financial_report] >> manual
```

**Patrones demostrados:**

1. **XCom automático:** `return` de @task se pushea, argumentos se pullea
2. **Múltiples consumidores:** base_metrics es usado por 4 tareas diferentes
3. **Cadena de XComs:** derived depende de base, financial depende de ambos
4. **XCom explícito:** `ti.xcom_pull()` y `ti.xcom_push()` con keys custom
5. **Procesamiento paralelo:** 3 reportes usan XComs en paralelo

**XCom en la práctica:**
- Almacenado en metadata DB de Airflow
- Limitado a 48KB por default (usar S3/GCS para objetos grandes)
- Cada tarea puede push/pull múltiples keys
- TaskFlow hace XCom transparente (menos código boilerplate)

**Configuración técnica:**
- DAG ID: `xcoms_challenge`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'xcoms']`
- 6 funciones @task demostrando XCom automático y manual
- Flujo con dependencias múltiples: 1 → N, N → 1, parallel
"""

import datetime

from airflow.sdk import DAG, task

# Solución del challenge

@task(task_id='extract_total_records')
def extract_records(**context):
    """Extrae y cuenta registros - XCom automático"""
    total = 5000
    print(f"Extracted {total} records")
    return total  # TaskFlow hace XCom push automáticamente

@task(task_id='extract_error_rate')
def extract_errors(**context):
    """Extrae tasa de errores - XCom automático"""
    error_rate = 0.03  # 3%
    print(f"Error rate: {error_rate}")
    return error_rate

@task(task_id='calculate_metrics')
def calculate_metrics(total_records, error_rate, **context):
    """Recibe múltiples XComs como argumentos"""
    # TaskFlow hace XCom pull automáticamente de las dependencias
    clean_records = int(total_records * (1 - error_rate))
    error_records = total_records - clean_records
    
    print(f"Total: {total_records}")
    print(f"Clean: {clean_records}")
    print(f"Errors: {error_records}")
    
    return {
        'total': total_records,
        'clean': clean_records,
        'errors': error_records,
        'error_rate': error_rate
    }

@task(task_id='push_multiple_values')
def push_multiple(**context):
    """XCom manual: push múltiples keys"""
    ti = context['task_instance']
    
    # Push manual de múltiples valores
    ti.xcom_push(key='database_name', value='warehouse_prod')
    ti.xcom_push(key='table_name', value='fact_sales')
    ti.xcom_push(key='partition_date', value='2024-01-01')
    
    print("Pushed 3 XCom keys: database_name, table_name, partition_date")
    return "metadata_pushed"  # Return también hace push con key='return_value'

@task(task_id='pull_specific_values')
def pull_specific(**context):
    """XCom manual: pull keys específicas"""
    ti = context['task_instance']
    
    # Pull manual de valores específicos
    db = ti.xcom_pull(task_ids='push_multiple_values', key='database_name')
    table = ti.xcom_pull(task_ids='push_multiple_values', key='table_name')
    partition = ti.xcom_pull(task_ids='push_multiple_values', key='partition_date')
    
    print(f"Loading to {db}.{table} partition {partition}")
    
    return {'target': f"{db}.{table}", 'partition': partition}

@task(task_id='generate_report')
def generate_report(metrics, load_info, **context):
    """Consolida múltiples XComs en reporte final"""
    # Recibe XComs de tareas anteriores como argumentos
    print("=== PROCESSING REPORT ===")
    print(f"Total records processed: {metrics['total']}")
    print(f"Clean records: {metrics['clean']}")
    print(f"Error records: {metrics['errors']}")
    print(f"Error rate: {metrics['error_rate']:.2%}")
    print(f"Loaded to: {load_info['target']}")
    print(f"Partition: {load_info['partition']}")
    
    return {
        'report_status': 'completed',
        'summary': metrics,
        'destination': load_info
    }

with DAG(
    dag_id='xcoms_challenge',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['challenge', 'xcoms'],
) as dag:
    
    # Extracciones paralelas
    total = extract_records()
    errors = extract_errors()
    
    # Calcula métricas (recibe 2 XComs)
    metrics = calculate_metrics(total, errors)
    
    # Push/pull manual
    metadata = push_multiple()
    load_info = pull_specific()
    
    # Reporte final (consolida todo)
    report = generate_report(metrics, load_info)
    
    # Dependencies
    [total, errors] >> metrics
    metadata >> load_info
    [metrics, load_info] >> report
