"""
Challenge: Un DAG por Cada Tabla de la Base de Datos

Tu data warehouse tiene 50 tablas. Cada una necesita un pipeline de quality checks. Podrías
crear 50 archivos de DAG manualmente, pero cada vez que agregan una tabla nueva, tienes que
crear otro DAG manualmente. Es tedioso, propenso a errores, y no escala.

La solución: defines la configuración de tablas en un diccionario (o podría ser YAML, base de datos,
API, lo que sea), y generas los 50 DAGs automáticamente con un loop. Agregas una tabla a la config,
y automáticamente aparece su DAG en Airflow. Quitas una, y desaparece.

Las tablas tienen diferentes requirements según su tipo:
- **Dimensionales** (dim_*): 3 checks diarios a las 00:00
- **Hechos** (fact_*): 5 checks cada hora (datos críticos)
- **Staging** (stg_*): 2 checks cada 30 minutos (ingesta continua)

**Configuración de tablas:**

```python
TABLE_CONFIG = {
    # Tablas dimensionales (cambian poco, checks diarios)
    'dim_customers': {
        'type': 'dimension',
        'schedule': '@daily',
        'checks': ['row_count', 'nulls', 'duplicates']
    },
    'dim_products': {
        'type': 'dimension',
        'schedule': '@daily',
        'checks': ['row_count', 'nulls', 'duplicates']
    },
    'dim_locations': {
        'type': 'dimension',
        'schedule': '@daily',
        'checks': ['row_count', 'nulls', 'duplicates']
    },
    
    # Tablas de hechos (cambian mucho, checks frecuentes)
    'fact_sales': {
        'type': 'fact',
        'schedule': '@hourly',
        'checks': ['row_count', 'nulls', 'duplicates', 'referential_integrity', 'data_freshness']
    },
    'fact_orders': {
        'type': 'fact',
        'schedule': '@hourly',
        'checks': ['row_count', 'nulls', 'duplicates', 'referential_integrity', 'data_freshness']
    },
    
    # Tablas staging (ingesta continua, checks rápidos)
    'stg_api_events': {
        'type': 'staging',
        'schedule': '*/30 * * * *',  # cada 30 min
        'checks': ['row_count', 'data_freshness']
    },
    'stg_kafka_stream': {
        'type': 'staging',
        'schedule': '*/30 * * * *',
        'checks': ['row_count', 'data_freshness']
    },
    
    # ... 43 tablas más ...
}
```

**Factory function que genera DAGs:**

```python
def create_quality_check_dag(table_name, table_config):
    dag_id = f"data_quality_{table_config['type']}_{table_name}"
    
    dag = DAG(
        dag_id=dag_id,
        schedule=table_config['schedule'],
        start_date=datetime.datetime(2024, 1, 1),
        catchup=False,
        tags=['challenge', 'dynamic_dags', table_config['type'], table_name]
    )
    
    with dag:
        start = EmptyOperator(task_id='start')
        
        # Generar tareas dinámicamente según checks
        check_tasks = []
        for check in table_config['checks']:
            task = BashOperator(
                task_id=f'check_{check}',
                bash_command=f'echo "Running {check} on {table_name}"'
            )
            check_tasks.append(task)
        
        end = EmptyOperator(task_id='end')
        
        # Dependencies
        start >> check_tasks >> end
    
    return dag
```

**Generación de todos los DAGs:**

```python
# Loop sobre la configuración y genera DAGs
for table_name, table_config in TABLE_CONFIG.items():
    dag_id = f"data_quality_{table_config['type']}_{table_name}"
    globals()[dag_id] = create_quality_check_dag(table_name, table_config)
```

Esto crea automáticamente DAGs como:
- `data_quality_dimension_dim_customers` (diario, 3 checks)
- `data_quality_dimension_dim_products` (diario, 3 checks)
- `data_quality_fact_fact_sales` (hourly, 5 checks)
- `data_quality_fact_fact_orders` (hourly, 5 checks)
- `data_quality_staging_stg_api_events` (cada 30min, 2 checks)
- ... 45 más ...

**El flujo de cada DAG generado:**

`start` >>

Tareas generadas dinámicamente según `table_config['checks']`:
- `check_row_count` (BashOperator - verifica count > 0 y creciendo)
- `check_nulls` (BashOperator - verifica columnas críticas sin nulls)
- `check_duplicates` (BashOperator - verifica primary keys únicos)
- `check_referential_integrity` (BashOperator - verifica foreign keys válidos) [solo fact tables]
- `check_data_freshness` (BashOperator - verifica datos recientes) [solo fact/staging]

>> `end`

**Ventajas del approach:**
- **Escalabilidad:** 1 tabla nueva = 1 entrada en dict, DAG aparece automático
- **Consistencia:** Todos los DAGs siguen el mismo patrón
- **Mantenibilidad:** Cambias la factory, 50 DAGs se actualizan
- **Configuration as Code:** La config es versionada junto al código
- **Flexibilidad:** Diferentes tipos de tablas tienen diferentes checks

**Evolución posible:**
En vez de dict hardcoded, podrías:
- Leer de YAML: `TABLE_CONFIG = yaml.load('tables.yaml')`
- Leer de DB: `TABLE_CONFIG = fetch_from_metadata_db()`
- Leer de API: `TABLE_CONFIG = requests.get('/api/tables').json()`

**Configuración técnica:**
- Genera al menos 10 DAGs (3 dimension, 4 fact, 3 staging)
- DAG IDs: `data_quality_{tipo}_{tabla}`
- Schedules diferenciados: daily para dim, hourly para fact, cada 30min para staging
- Tags dinámicos: `['challenge', 'dynamic_dags', tipo, tabla]`
- Cada DAG tiene 2-5 tareas según tipo
- Total: 10 DAGs generados con ~30 líneas de código
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Solución del challenge

# Configuración de tablas (normalmente vendría de archivo/DB/API)
TABLE_CONFIG = [
    # Dimension tables
    {'name': 'customers', 'type': 'dimension', 'owner': 'crm_team'},
    {'name': 'products', 'type': 'dimension', 'owner': 'product_team'},
    {'name': 'locations', 'type': 'dimension', 'owner': 'ops_team'},
    
    # Fact tables
    {'name': 'orders', 'type': 'fact', 'owner': 'sales_team'},
    {'name': 'payments', 'type': 'fact', 'owner': 'finance_team'},
    {'name': 'shipments', 'type': 'fact', 'owner': 'logistics_team'},
    {'name': 'returns', 'type': 'fact', 'owner': 'support_team'},
    
    # Staging tables
    {'name': 'raw_web_events', 'type': 'staging', 'owner': 'analytics_team'},
    {'name': 'raw_api_logs', 'type': 'staging', 'owner': 'engineering_team'},
    {'name': 'raw_mobile_events', 'type': 'staging', 'owner': 'mobile_team'},
]

# Configuración por tipo de tabla
TYPE_CONFIG = {
    'dimension': {
        'schedule': '@daily',
        'checks': ['row_count', 'uniqueness', 'nulls']
    },
    'fact': {
        'schedule': '@hourly',
        'checks': ['row_count', 'freshness', 'foreign_keys', 'aggregations']
    },
    'staging': {
        'schedule': '*/30 * * * *',
        'checks': ['row_count', 'schema_validation']
    }
}

# Generar un DAG por cada tabla
for table in TABLE_CONFIG:
    table_name = table['name']
    table_type = table['type']
    owner = table['owner']
    
    config = TYPE_CONFIG[table_type]
    dag_id = f"data_quality_{table_type}_{table_name}"
    
    with DAG(
        dag_id=dag_id,
        start_date=datetime.datetime(2024, 1, 1),
        schedule=config['schedule'],
        catchup=False,
        tags=['challenge', 'dynamic_dags', table_type, table_name],
    ) as dag:
        
        start = EmptyOperator(task_id='start')
        
        # Crear tareas dinámicamente según el tipo de tabla
        check_tasks = []
        for check in config['checks']:
            task = BashOperator(
                task_id=f'check_{check}',
                bash_command=f'echo "Running {check} check on {table_name}"',
            )
            check_tasks.append(task)
        
        notify = BashOperator(
            task_id='notify_owner',
            bash_command=f'echo "Notifying {owner} about {table_name}"',
        )
        
        end = EmptyOperator(task_id='end')
        
        # Dependencies
        start >> check_tasks >> notify >> end
    
    # Registrar el DAG en globals() para que Airflow lo detecte
    globals()[dag_id] = dag
