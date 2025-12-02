"""
Params - Conditional Logic con Params

Usa params para controlar flujo del pipeline.
Habilita/deshabilita features según configuración.
"""

import datetime

from airflow.sdk import DAG, task


@task
def extract_data(**context):
    """Extrae datos"""
    params = context['params']
    
    print("📥 Extrayendo datos...")
    data = [{'id': i, 'value': i * 10} for i in range(1, 11)]
    
    print(f"✅ Extraídos {len(data)} records")
    return {'records': data, 'count': len(data)}


@task
def conditional_validation(data: dict, **context):
    """Valida solo si param enabled"""
    params = context['params']
    
    if not params['enable_validation']:
        print("⚠️ Validation disabled, skipping")
        return data
    
    print("🔍 Running validation...")
    # Validate
    valid_records = [r for r in data['records'] if r['value'] > 0]
    
    print(f"✅ Validated: {len(valid_records)} valid records")
    return {'records': valid_records, 'count': len(valid_records)}


@task
def conditional_enrichment(data: dict, **context):
    """Enriquece solo si param enabled"""
    params = context['params']
    
    if not params['enable_enrichment']:
        print("⚠️ Enrichment disabled, skipping")
        return data
    
    print("⚙️ Enriching data...")
    for record in data['records']:
        record['enriched'] = True
        record['category'] = 'high' if record['value'] > 50 else 'low'
    
    print(f"✅ Enriched {data['count']} records")
    return data


@task
def load_data(data: dict, **context):
    """Carga a destination según param"""
    params = context['params']
    destination = params['destination']
    
    print(f"💾 Loading to {destination}...")
    print(f"  - Records: {data['count']}")
    
    return {'status': 'loaded', 'destination': destination, 'count': data['count']}


with DAG(
    dag_id='params_conditional_logic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    params={
        'enable_validation': True,
        'enable_enrichment': True,
        'destination': 'warehouse'
    },
    tags=['example', 'params']
) as dag:
    
    data = extract_data()
    validated = conditional_validation(data)
    enriched = conditional_enrichment(validated)
    result = load_data(enriched)

dag.doc_md = """
# Conditional Logic con Params

## Pattern:
```python
@task
def conditional_task(data, **context):
    if not context['params']['enable_feature']:
        return data  # Skip
    
    # Feature logic
    return process(data)
```

## Trigger con features:
```bash
# Full pipeline
airflow dags trigger my_dag --conf '{
  "enable_validation": true,
  "enable_enrichment": true
}'

# Skip enrichment
airflow dags trigger my_dag --conf '{
  "enable_validation": true,
  "enable_enrichment": false
}'
```

Útil para A/B testing, feature toggles, debugging.
"""
