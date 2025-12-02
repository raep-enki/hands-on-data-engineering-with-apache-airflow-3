"""
Variables - Environment-Specific Configuration

Usa variables para configurar comportamiento según environment.
Patrón común: variable 'environment' controla otras configs.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.models import Variable


@task
def get_environment_config():
    """Lee configuración según environment"""
    env = Variable.get("environment", default_var="production")
    
    print(f"🌍 Environment: {env}")
    
    # Configuración por environment
    configs = {
        "development": {
            "db_host": "localhost",
            "db_pool_size": 5,
            "api_timeout": 10,
            "debug": True,
            "log_level": "DEBUG"
        },
        "staging": {
            "db_host": "staging-db.example.com",
            "db_pool_size": 10,
            "api_timeout": 30,
            "debug": True,
            "log_level": "INFO"
        },
        "production": {
            "db_host": "prod-db.example.com",
            "db_pool_size": 20,
            "api_timeout": 60,
            "debug": False,
            "log_level": "WARNING"
        }
    }
    
    config = configs.get(env, configs["production"])
    print(f"  - Config: {config}")
    
    return {"environment": env, "config": config}


@task
def process_with_env_config(env_data: dict):
    """Procesa datos usando configuración de environment"""
    env = env_data["environment"]
    config = env_data["config"]
    
    print(f"⚙️ Procesando en {env}...")
    print(f"  - DB: {config['db_host']}")
    print(f"  - Pool size: {config['db_pool_size']}")
    print(f"  - Debug: {config['debug']}")
    
    if config["debug"]:
        print("  🐛 Debug mode: verbose logging enabled")
    
    return {"status": "processed", "environment": env}


with DAG(
    dag_id='variables_environment_config',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'variables']
) as dag:
    
    config = get_environment_config()
    process = process_with_env_config(config)

dag.doc_md = """
# Environment-Specific Config

## Pattern:
```python
env = Variable.get("environment", default_var="production")

if env == "development":
    use_test_data()
elif env == "production":
    use_prod_data()
```

## Setup:
```bash
# Development
airflow variables set environment development

# Production
airflow variables set environment production
```

Útil para: dev/staging/prod, feature flags, A/B testing.
"""
