"""
Params - Validación de Params

Valida params para asegurar valores correctos antes de procesar.
Importante para robustez del pipeline.
"""

import datetime

from airflow.sdk import DAG, task


@task
def validate_params(**context):
    """Valida params antes de usar"""
    params = context['params']
    
    print("🔍 Validando params...")
    
    errors = []
    
    # Validar environment
    valid_envs = ['development', 'staging', 'production']
    if params['environment'] not in valid_envs:
        errors.append(f"Invalid environment: {params['environment']}")
    
    # Validar batch_size
    if params['batch_size'] <= 0:
        errors.append(f"batch_size must be > 0: {params['batch_size']}")
    
    if params['batch_size'] > 100000:
        errors.append(f"batch_size too large: {params['batch_size']}")
    
    # Validar timeout
    if params['timeout_seconds'] < 10:
        errors.append(f"timeout too low: {params['timeout_seconds']}")
    
    if errors:
        error_msg = "Param validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    
    print("✅ Params válidos")
    return {'valid': True, 'params': params}


@task
def process_with_validated_params(validation: dict):
    """Procesa con params ya validados"""
    params = validation['params']
    
    print(f"⚙️ Procesando con params validados:")
    print(f"  - Environment: {params['environment']}")
    print(f"  - Batch size: {params['batch_size']}")
    print(f"  - Timeout: {params['timeout_seconds']}s")
    
    return {'status': 'processed', 'params_used': params}


with DAG(
    dag_id='params_validation',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    params={
        'environment': 'production',
        'batch_size': 1000,
        'timeout_seconds': 300
    },
    tags=['example', 'core_concepts', 'params']
) as dag:
    
    validated = validate_params()
    result = process_with_validated_params(validated)

dag.doc_md = """
# Validación de Params

## Pattern:
```python
@task
def validate_params(**context):
    params = context['params']
    errors = []
    
    if params['value'] < 0:
        errors.append("value must be positive")
    
    if errors:
        raise ValueError("\\n".join(errors))
    
    return params
```

## Validaciones típicas:
- Rangos numéricos
- Valores permitidos (enums)
- Formatos (dates, URLs)
- Dependencies entre params

Validar early para fail fast.
"""
