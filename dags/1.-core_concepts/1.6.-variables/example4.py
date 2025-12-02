"""
Variables - Feature Flags con Variables

Usa variables como feature flags para habilitar/deshabilitar
funcionalidades sin cambiar código.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.models import Variable


@task
def check_feature_flags():
    """Lee feature flags de variables"""
    print("🚩 Verificando feature flags...")
    
    # Leer flags (default False si no existen)
    enable_ml = Variable.get("feature_ml_predictions", default_var="false") == "true"
    enable_cache = Variable.get("feature_caching", default_var="true") == "true"
    enable_notifications = Variable.get("feature_notifications", default_var="true") == "true"
    
    flags = {
        "ml_predictions": enable_ml,
        "caching": enable_cache,
        "notifications": enable_notifications
    }
    
    print(f"  - Flags: {flags}")
    return flags


@task
def extract_data(flags: dict):
    """Extrae datos con caching condicional"""
    print("📥 Extrayendo datos...")
    
    data = {"records": [1, 2, 3], "count": 3}
    
    if flags["caching"]:
        print("  ✅ Cache enabled: checking cache...")
        # cache_result = check_cache()
        print("  📦 Cache miss, extracting from source")
    else:
        print("  ⚠️ Cache disabled: extracting directly")
    
    return data


@task
def process_data(data: dict, flags: dict):
    """Procesa datos con ML condicional"""
    print(f"⚙️ Procesando {data['count']} records...")
    
    processed = data
    
    if flags["ml_predictions"]:
        print("  🤖 ML enabled: running predictions...")
        # predictions = ml_model.predict(data)
        processed["predictions"] = [10, 20, 30]
    else:
        print("  ⚠️ ML disabled: skipping predictions")
    
    return processed


@task
def load_data(data: dict, flags: dict):
    """Carga datos con notificaciones condicionales"""
    print(f"💾 Cargando {data['count']} records...")
    
    # Simulate load
    print("  ✅ Data loaded")
    
    if flags["notifications"]:
        print("  📧 Notifications enabled: sending alerts...")
        # send_notification("Data loaded successfully")
    else:
        print("  ⚠️ Notifications disabled")
    
    return {"status": "success", "count": data["count"]}


with DAG(
    dag_id='variables_feature_flags',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'variables']
) as dag:
    
    flags = check_feature_flags()
    data = extract_data(flags)
    processed = process_data(data, flags)
    result = load_data(processed, flags)

dag.doc_md = """
# Feature Flags

## Setup flags:
```bash
airflow variables set feature_ml_predictions true
airflow variables set feature_caching true
airflow variables set feature_notifications false
```

## Check flags:
```python
enable_ml = Variable.get("feature_ml", default_var="false") == "true"

if enable_ml:
    run_ml_model()
```

## Ventajas:
- Toggle features sin deploy
- A/B testing
- Gradual rollout
- Quick disable si problemas
"""
