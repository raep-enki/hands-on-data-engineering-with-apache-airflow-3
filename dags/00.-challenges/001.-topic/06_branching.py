"""
Challenge: Pipeline que Toma Decisiones Inteligentes

Tu empresa procesa datos de clientes de múltiples regiones (Norteamérica, Europa, Asia-Pacífico)
y cada región necesita un procesamiento diferente. Además, necesitas un branching condicional
basado en la hora del día (horario pico vs normal), y otro basado en el día de la semana
(fin de semana vs días laborables). Son tres niveles de decisiones encadenadas.

El sistema debe elegir automáticamente el path correcto según las condiciones, pero **NO DEBES USAR
XComs** (ese tema viene más adelante). Las decisiones se basan en cálculos simples dentro de cada
función de branching: hora actual del sistema, día de la semana, un simple módulo para simular región.

**Las tareas y el flujo:**

Comienzas con `start` (EmptyOperator) → `branch_by_region` (BranchPythonOperator que decide región).

**Primera decisión - por región (`branch_by_region`):**
Usa `datetime.now().day % 3` para simular región (0=NA, 1=EU, 2=APAC):
- Si 0 → `process_north_america` (BashOperator que procesa USA/Canadá)
- Si 1 → `process_europe` (BashOperator aplica reglas UE)
- Si 2 → `process_asia_pacific` (BashOperator aplica reglas APAC)

Todas convergen en `aggregate_all_regions` (BashOperator consolida).

**Segunda decisión - por hora (`branch_by_time`):**
Usa `datetime.now().hour` (si >= 9 y < 18 es pico, si no es normal):
- Si hora pico → `high_load_processing` (BashOperator con procesamiento intenso)
- Si hora normal → `low_load_processing` (BashOperator con procesamiento ligero)

Ambas convergen en `validate_output` (BashOperator valida).

**Tercera decisión - por día (`branch_by_weekday`):**
Usa `datetime.now().weekday()` (si < 5 es laboral, si no es weekend):
- Si laboral → `standard_delivery` (BashOperator entrega normal)
- Si weekend → `priority_delivery` (BashOperator entrega express)

Ambas convergen en `send_completion_report` (debe tener `trigger_rule='none_failed_min_one_success'`
porque viene de paths diferentes) → `end` (EmptyOperator).

**Funciones de branching:**
Crea 3 funciones Python que calculen la decisión localmente sin XComs:
```python
def branch_by_region(**context):
    region_code = datetime.now().day % 3
    if region_code == 0: return 'process_north_america'
    elif region_code == 1: return 'process_europe'
    else: return 'process_asia_pacific'
```

**Configuración técnica:**
- DAG ID: `branching_challenge`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'branching']`
- Todas las tareas post-branch: `trigger_rule='none_failed_min_one_success'`
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Solución del challenge

# Funciones de branching (sin XComs, solo usando datetime)
def branch_by_region(**context):
    region_code = datetime.datetime.now().day % 3
    if region_code == 0:
        return 'process_north_america'
    elif region_code == 1:
        return 'process_europe'
    else:
        return 'process_asia_pacific'

def branch_by_time(**context):
    hour = datetime.datetime.now().hour
    if 9 <= hour < 18:
        return 'high_load_processing'
    else:
        return 'low_load_processing'

def branch_by_weekday(**context):
    weekday = datetime.datetime.now().weekday()
    if weekday < 5:
        return 'standard_delivery'
    else:
        return 'priority_delivery'

with DAG(
    dag_id='branching_challenge',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['challenge', 'branching'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Primera decisión: por región
    branch_by_region_task = BranchPythonOperator(
        task_id='branch_by_region',
        python_callable=branch_by_region,
    )
    
    process_north_america = BashOperator(
        task_id='process_north_america',
        bash_command='echo "Processing North America region"',
    )
    
    process_europe = BashOperator(
        task_id='process_europe',
        bash_command='echo "Processing Europe region"',
    )
    
    process_asia_pacific = BashOperator(
        task_id='process_asia_pacific',
        bash_command='echo "Processing Asia-Pacific region"',
    )
    
    aggregate_all_regions = BashOperator(
        task_id='aggregate_all_regions',
        bash_command='echo "Aggregating all regions"',
        trigger_rule='none_failed_min_one_success',
    )
    
    # Segunda decisión: por hora del día
    branch_by_time_task = BranchPythonOperator(
        task_id='branch_by_time',
        python_callable=branch_by_time,
    )
    
    high_load_processing = BashOperator(
        task_id='high_load_processing',
        bash_command='echo "High load processing (peak hours)"',
    )
    
    low_load_processing = BashOperator(
        task_id='low_load_processing',
        bash_command='echo "Low load processing (normal hours)"',
    )
    
    validate_output = BashOperator(
        task_id='validate_output',
        bash_command='echo "Validating output"',
        trigger_rule='none_failed_min_one_success',
    )
    
    # Tercera decisión: por día de la semana
    branch_by_weekday_task = BranchPythonOperator(
        task_id='branch_by_weekday',
        python_callable=branch_by_weekday,
    )
    
    standard_delivery = BashOperator(
        task_id='standard_delivery',
        bash_command='echo "Standard delivery (weekday)"',
    )
    
    priority_delivery = BashOperator(
        task_id='priority_delivery',
        bash_command='echo "Priority delivery (weekend)"',
    )
    
    send_completion_report = BashOperator(
        task_id='send_completion_report',
        bash_command='echo "Sending completion report"',
        trigger_rule='none_failed_min_one_success',
    )
    
    end = EmptyOperator(task_id='end')
    
    # Dependencies
    start >> branch_by_region_task
    branch_by_region_task >> [process_north_america, process_europe, process_asia_pacific]
    [process_north_america, process_europe, process_asia_pacific] >> aggregate_all_regions
    
    aggregate_all_regions >> branch_by_time_task
    branch_by_time_task >> [high_load_processing, low_load_processing]
    [high_load_processing, low_load_processing] >> validate_output
    
    validate_output >> branch_by_weekday_task
    branch_by_weekday_task >> [standard_delivery, priority_delivery]
    [standard_delivery, priority_delivery] >> send_completion_report
    
    send_completion_report >> end
