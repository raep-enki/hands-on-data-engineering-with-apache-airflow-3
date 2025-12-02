"""
Dynamic Task Mapping - Filtrado con expand_kwargs()

expand_kwargs() permite pasar múltiples kwargs dinámicamente.
Útil para configuraciones complejas por task instance.
"""

import datetime

from airflow.sdk import DAG, task


@task
def generate_jobs():
    """Genera lista de jobs con configuración completa"""
    print("⚙️ Generando jobs...")
    
    jobs = [
        {
            'job_name': 'etl_users',
            'source': 'api',
            'batch_size': 1000,
            'timeout': 300
        },
        {
            'job_name': 'etl_orders',
            'source': 'database',
            'batch_size': 1500,
            'timeout': 400
        },
        {
            'job_name': 'etl_products',
            'source': 's3',
            'batch_size': 1200,
            'timeout': 350
        }
    ]
    
    print(f"✅ {len(jobs)} jobs generados")
    return jobs


@task
def run_job(job_name: str, source: str, batch_size: int, timeout: int):
    """Ejecuta un job con su configuración específica"""
    print(f"🚀 Ejecutando job: {job_name}")
    print(f"  - Source: {source}")
    print(f"  - Batch size: {batch_size}")
    print(f"  - Timeout: {timeout}s")
    
    # Simular ejecución
    records = batch_size * 3
    
    return {
        'job_name': job_name,
        'source': source,
        'records': records,
        'status': 'completed'
    }


@task
def report_jobs(results: list):
    """Genera reporte de todos los jobs"""
    print(f"📊 Reporte de {len(results)} jobs:")
    
    for result in results:
        print(f"  - {result['job_name']}: {result['records']} records ({result['status']})")
    
    total_records = sum(r['records'] for r in results)
    
    return {
        'jobs_count': len(results),
        'total_records': total_records,
        'all_completed': all(r['status'] == 'completed' for r in results)
    }


with DAG(
    dag_id='dynamic_mapping_expand_kwargs',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'simple_mapping']
) as dag:
    
    jobs = generate_jobs()
    
    # expand_kwargs: cada dict se expande como **kwargs
    executed = run_job.expand_kwargs(jobs)
    
    report = report_jobs(executed)

dag.doc_md = """
# expand_kwargs() - Múltiples Kwargs Dinámicos

## expand vs expand_kwargs:

### expand (un argumento):
```python
@task
def process(item: int):
    pass

items = [1, 2, 3]
process.expand(item=items)
```

### expand_kwargs (múltiples kwargs):
```python
@task
def process(name: str, size: int, timeout: int):
    pass

configs = [
    {'name': 'job1', 'size': 100, 'timeout': 300},
    {'name': 'job2', 'size': 200, 'timeout': 400}
]

# Cada dict se expande como **kwargs
process.expand_kwargs(configs)

# Equivalente a:
# process(name='job1', size=100, timeout=300)
# process(name='job2', size=200, timeout=400)
```

## Ventajas:
- Configuración compleja por task
- Cada task instance tiene kwargs diferentes
- Flexible y expresivo

## Cuándo usar:
- Cada task necesita múltiples params únicos
- Configuración heterogénea entre tasks
- Jobs con diferentes settings

## Combine con partial():
```python
process.partial(
    common_param='value'
).expand_kwargs(configs)
```
"""
