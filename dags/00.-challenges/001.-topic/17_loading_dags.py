"""
Challenge: 16 DAGs Casi Idénticos (Escribir Menos, Lograr Más)

Tu empresa tiene 4 regiones (US, EU, APAC, LATAM) y 4 departamentos (Sales, Marketing, Finance, Operations).
Cada combinación necesita su propio pipeline ETL. Eso son 4 × 4 = **16 DAGs**. Podrías copiar-pegar
código 16 veces, pero eso es una pesadilla de mantenimiento: si cambias la lógica de validación,
tienes que editarlo 16 veces.

La solución: una **factory function** que genere DAGs dinámicamente. Le pasas región y departamento,
y te devuelve un DAG configurado. Cambias una línea en la factory, y los 16 se actualizan.
Es el principio DRY (Don't Repeat Yourself) aplicado a pipelines.

**Características diferenciadas por departamento:**

- **Sales**: Crítico, corre cada hora, timeout 30 min
- **Marketing**: Importante, corre cada 6 horas, timeout 2 hrs
- **Finance**: Regulado, corre diario a las 02:00 AM, timeout 4 hrs
- **Operations**: Batch, corre diario a las 01:00 AM, timeout 6 hrs

**El flujo base (igual para todos, pero parametrizado):**

`start` >> `extract_from_source` (BashOperator - extrae de sistema legacy de la región,
comando incluye nombre de región: `aws s3 cp s3://data-{region}/ ...`) >>

`validate_raw_data` (BashOperator - verifica schema, nulls, duplicados. Timeout varía por depto) >>

`transform_data` (BashOperator - aplica business rules del departamento. Usa config específica:
Sales calcula comisiones, Marketing calcula ROI, Finance calcula impuestos, Operations calcula costos) >>

`quality_check` (BashOperator - valida output cumple estándares. QA más estricto para Finance) >>

`load_to_warehouse` (BashOperator - carga a Snowflake tabla: `{region}_{depto}_data`) >>

`notify_completion` (BashOperator - envía email a equipo correspondiente) >> `end`

**La factory function debe verse así:**

```python
def create_etl_dag(region, department):
    # Config específica por departamento
    dept_config = {
        'sales': {'schedule': '@hourly', 'timeout': 30},
        'marketing': {'schedule': '0 */6 * * *', 'timeout': 120},
        'finance': {'schedule': '0 2 * * *', 'timeout': 240},
        'operations': {'schedule': '0 1 * * *', 'timeout': 360}
    }
    
    config = dept_config[department.lower()]
    
    dag = DAG(
        dag_id=f'etl_{region.lower()}_{department.lower()}',
        schedule=config['schedule'],
        start_date=datetime.datetime(2024, 1, 1),
        catchup=False,
        tags=['challenge', 'loading_dags', region.lower(), department.lower()]
    )
    
    with dag:
        start = EmptyOperator(task_id='start')
        
        extract = BashOperator(
            task_id='extract_from_source',
            bash_command=f'echo "Extracting from {region} {department}"',
            execution_timeout=timedelta(minutes=config['timeout'])
        )
        
        # ... demás tareas
        
        start >> extract >> ... >> end
    
    return dag
```

**Generación de los 16 DAGs:**

```python
REGIONS = ['US', 'EU', 'APAC', 'LATAM']
DEPARTMENTS = ['Sales', 'Marketing', 'Finance', 'Operations']

for region in REGIONS:
    for department in DEPARTMENTS:
        dag_id = f'etl_{region.lower()}_{department.lower()}'
        globals()[dag_id] = create_etl_dag(region, department)
```

Esto crea automáticamente:
- `etl_us_sales`, `etl_us_marketing`, `etl_us_finance`, `etl_us_operations`
- `etl_eu_sales`, `etl_eu_marketing`, `etl_eu_finance`, `etl_eu_operations`
- `etl_apac_sales`, `etl_apac_marketing`, `etl_apac_finance`, `etl_apac_operations`
- `etl_latam_sales`, `etl_latam_marketing`, `etl_latam_finance`, `etl_latam_operations`

**Por qué funciona:**
- Airflow busca objetos DAG en el namespace global (`globals()`)
- La factory crea un DAG nuevo con cada combinación
- `globals()[dag_id] = ...` registra el DAG en el namespace
- Airflow los detecta automáticamente

**Ventajas:**
- **Mantenibilidad:** Cambias la factory, todos se actualizan
- **Consistencia:** Todos siguen el mismo patrón
- **Escalabilidad:** Agregar nueva región = 1 línea en la lista
- **Testing:** Pruebas la factory 1 vez, aplica a todos

**Configuración técnica:**
- 16 DAGs generados dinámicamente
- DAG IDs: `etl_{region}_{department}` (minúsculas)
- Schedules diferenciados: Sales hourly, Marketing every 6hrs, Finance/Ops daily
- Tags dinámicos: `['challenge', 'loading_dags', region, department]`
- Cada DAG tiene 6 tareas con configuraciones parametrizadas
- Total: 16 DAGs × 6 tareas = 96 tareas gestionadas con ~50 líneas de código
"""

import datetime
import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Crea la factory function que genera múltiples DAGs
