"""
Variables - Best Practices y Seguridad

Demuestra buenas prácticas: defaults, validación, y NO guardar secrets.
Para secrets, usar Airflow Connections o Secret Backend.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.models import Variable


@task
def read_variables_safely():
    """Lee variables con defaults y validación"""
    print("🔐 Leyendo variables de forma segura...")
    
    # ✅ Siempre con default
    batch_size = int(Variable.get("batch_size", default_var="1000"))
    timeout = int(Variable.get("timeout_seconds", default_var="300"))
    
    # Validación
    if batch_size <= 0:
        print("  ⚠️ Invalid batch_size, using default")
        batch_size = 1000
    
    if timeout < 10:
        print("  ⚠️ Timeout too low, using minimum")
        timeout = 10
    
    print(f"  - Batch size: {batch_size}")
    print(f"  - Timeout: {timeout}s")
    
    return {"batch_size": batch_size, "timeout": timeout}


@task
def bad_practices_demo():
    """❌ Ejemplos de malas prácticas (NO HACER)"""
    print("⚠️ Ejemplos de lo que NO hacer...")
    
    # ❌ NO guardar passwords en Variables
    # password = Variable.get("database_password")  # MAL
    
    # ❌ NO guardar API keys sensibles
    # api_key = Variable.get("stripe_api_key")  # MAL
    
    # ❌ NO guardar secrets
    # secret_token = Variable.get("jwt_secret")  # MAL
    
    print("  ❌ NO usar Variables para secrets!")
    print("  ✅ Usar Connections o Secret Backend")
    
    return {"warning": "Use Connections for secrets"}


@task
def good_practices_demo():
    """✅ Ejemplos de buenas prácticas"""
    print("✅ Buenas prácticas...")
    
    # ✅ Configuración no sensible
    region = Variable.get("aws_region", default_var="us-east-1")
    environment = Variable.get("environment", default_var="production")
    
    # ✅ Feature flags
    enable_feature = Variable.get("feature_x", default_var="false") == "true"
    
    # ✅ Configuración de comportamiento
    max_retries = int(Variable.get("max_retries", default_var="3"))
    
    print(f"  ✅ Region: {region}")
    print(f"  ✅ Environment: {environment}")
    print(f"  ✅ Feature X: {enable_feature}")
    print(f"  ✅ Max retries: {max_retries}")
    
    return {
        "region": region,
        "environment": environment,
        "feature_x": enable_feature,
        "max_retries": max_retries
    }


with DAG(
    dag_id='variables_best_practices',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'variables']
) as dag:
    
    safe = read_variables_safely()
    bad = bad_practices_demo()
    good = good_practices_demo()
    
    [safe, bad, good]

dag.doc_md = """
# Variables Best Practices

## ✅ Usar Variables para:
- Configuración no sensible
- Environment flags (dev/staging/prod)
- Feature flags
- Configuración de comportamiento (batch sizes, timeouts)
- URLs públicas, regions

## ❌ NO usar Variables para:
- Passwords
- API keys
- Tokens
- Certificates
- Cualquier secret

## Para secrets, usar:

### Airflow Connections:
```python
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('my_db')
password = conn.password  # Encriptado
```

### Secret Backend (AWS Secrets Manager, Vault, etc.):
```python
# Configurado en airflow.cfg
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
```

## Validación:
```python
batch_size = int(Variable.get("batch_size", default_var="1000"))
if batch_size <= 0:
    batch_size = 1000  # Fallback
```

## Performance:
Variables se leen de DB cada vez. Para lectura frecuente, cachear:
```python
# Read once
config = Variable.get("config", deserialize_json=True)
# Use many times
db_host = config['db']['host']
```
"""
