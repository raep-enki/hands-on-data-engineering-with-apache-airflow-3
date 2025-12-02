"""
Challenge: Funciones Compartidas Entre DAGs

Tienes 10 DAGs que validan datos de AWS, otros 8 que validan GCP, todos usan la misma lógica
de validación. Actualmente, esa lógica está duplicada 18 veces. Cambiar algo implica editar
18 archivos diferentes. Es insostenible y propenso a bugs.

La solución: crear un directorio `dags/config/` con módulos compartidos (validators.py,
connectors.py, transformers.py). Todos los DAGs importan de ahí. Cambias una línea en el módulo,
y los 18 DAGs se actualizan automáticamente.

El problema: Airflow intenta parsear TODO como DAGs. Si pones validators.py en dags/, Airflow
lo abre y dice "no encontré DAGs acá, error". Necesitas un `.airflowignore` que le diga
"no mires en config/, esos son helpers, no DAGs".

**Estructura de directorios:**

```
dags/
  .airflowignore          # Dice a Airflow qué ignorar
  multi_cloud_etl.py      # El DAG principal
  config/                 # Módulos compartidos
    __init__.py           # Para que sea paquete Python
    validators.py         # Funciones de validación
    connectors.py         # Funciones de conexión AWS/GCP
    transformers.py       # Funciones de transformación
```

**Contenido de .airflowignore:**

```
config/
__pycache__
*.pyc
.git/
README.md
```

Esto le dice a Airflow: "ignora config/ completo, ignora archivos compilados, ignora git".

**Tu tarea:**

Crea módulos compartidos en config/ (validators.py, connectors.py, transformers.py) y un DAG
multi_cloud_etl.py que los importe y use. El DAG debe procesar datos de AWS y GCP usando
funciones compartidas.

def extract_from_aws(**context):
    conn = connect_to_aws('us-east-1')
    print(f"Extracting from AWS using {conn}")

def validate_aws_data(**context):
    validate_schema(None, ['id', 'name', 'amount'])
    validate_nulls(None, ['id', 'name'])

def transform_aws_data(**context):
    normalize_dates(None)
    convert_currency(None, 'USD', 'EUR')

# Similar para GCP...

with DAG(
    dag_id='multi_cloud_etl',
    schedule='@daily',
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    tags=['challenge', 'packaging_dags']
) as dag:
    # Tareas usando helpers
    ...
```

**El flujo del DAG:**

`start` >>

**Branch AWS:**
`extract_aws` (PythonOperator usa `connect_to_aws()`) >>
`validate_aws` (PythonOperator usa `validate_schema()`, `validate_nulls()`) >>
`transform_aws` (PythonOperator usa `normalize_dates()`, `convert_currency()`) >>
`load_aws` (PythonOperator carga a warehouse) >>

**Branch GCP (paralelo):**
`extract_gcp` (PythonOperator usa `connect_to_gcp()`) >>
`validate_gcp` (PythonOperator usa `validate_schema()`, `validate_nulls()`) >>
`transform_gcp` (PythonOperator usa `normalize_dates()`, `convert_currency()`) >>
`load_gcp` (PythonOperator carga a warehouse) >>

**Merge:**
`[load_aws, load_gcp]` >> `reconcile_data` (PythonOperator compara ambas fuentes) >> `end`

**Ventajas del approach:**
- **DRY:** Validadores definidos 1 vez, usados N veces
- **Testable:** Puedes unit test los helpers independientes
- **Mantenible:** Bug fix en 1 lugar, aplica a todos
- **Organizado:** Código estructurado por responsabilidad

**Configuración técnica:**
- DAG ID: `multi_cloud_etl`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'packaging_dags']`
- Crear: `dags/config/` con 3 módulos Python
- Crear: `dags/.airflowignore` que excluya `config/`
- 10 tareas usando funciones compartidas
"""

import datetime
import sys
import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Solución del challenge
# Nota: En producción, crearías módulos reales en dags/config/
# Por ahora, definimos las funciones aquí simulando imports

# Simulación de: from config.cloud_connectors import AWSConnector, AzureConnector, GCPConnector
class AWSConnector:
    @staticmethod
    def connect(**context):
        print("Connecting to AWS S3")
        return {"provider": "AWS", "bucket": "s3://data-lake"}
    
    @staticmethod
    def extract(connection, **context):
        print(f"Extracting from {connection['bucket']}")
        return {"records": 1000, "source": "AWS"}

class AzureConnector:
    @staticmethod
    def connect(**context):
        print("Connecting to Azure Blob")
        return {"provider": "Azure", "container": "azure://data-container"}
    
    @staticmethod
    def extract(connection, **context):
        print(f"Extracting from {connection['container']}")
        return {"records": 800, "source": "Azure"}

class GCPConnector:
    @staticmethod
    def connect(**context):
        print("Connecting to GCP Storage")
        return {"provider": "GCP", "bucket": "gs://data-bucket"}
    
    @staticmethod
    def extract(connection, **context):
        print(f"Extracting from {connection['bucket']}")
        return {"records": 1200, "source": "GCP"}

# Simulación de: from config.data_quality import validate_schema, check_nulls, check_duplicates
def validate_schema(data, **context):
    print(f"Validating schema for {data.get('source', 'unknown')} data")
    return True

def check_nulls(data, **context):
    print(f"Checking nulls in {data.get('records', 0)} records")
    return {"null_count": 5}

def check_duplicates(data, **context):
    print(f"Checking duplicates in {data.get('records', 0)} records")
    return {"duplicate_count": 2}

# Simulación de: from config.transformations import normalize_data, enrich_with_metadata
def normalize_data(aws_data, azure_data, gcp_data, **context):
    print(f"Normalizing data from 3 sources")
    total = aws_data.get('records', 0) + azure_data.get('records', 0) + gcp_data.get('records', 0)
    return {"normalized_records": total}

def enrich_with_metadata(normalized, **context):
    logical_date = context['logical_date']
    print(f"Enriching {normalized['normalized_records']} records with metadata")
    return {"enriched_records": normalized['normalized_records'], "processed_at": str(logical_date)}

# DAG principal
with DAG(
    dag_id='multi_cloud_etl',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['challenge', 'packaging_dags'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Conexiones a clouds (usando módulos compartidos)
    connect_aws = PythonOperator(
        task_id='connect_aws',
        python_callable=AWSConnector.connect,
    )
    
    connect_azure = PythonOperator(
        task_id='connect_azure',
        python_callable=AzureConnector.connect,
    )
    
    connect_gcp = PythonOperator(
        task_id='connect_gcp',
        python_callable=GCPConnector.connect,
    )
    
    # Extracciones paralelas
    extract_aws = PythonOperator(
        task_id='extract_aws',
        python_callable=lambda **context: AWSConnector.extract(
            AWSConnector.connect(**context), **context
        ),
    )
    
    extract_azure = PythonOperator(
        task_id='extract_azure',
        python_callable=lambda **context: AzureConnector.extract(
            AzureConnector.connect(**context), **context
        ),
    )
    
    extract_gcp = PythonOperator(
        task_id='extract_gcp',
        python_callable=lambda **context: GCPConnector.extract(
            GCPConnector.connect(**context), **context
        ),
    )
    
    # Validaciones (usando módulo data_quality)
    validate = PythonOperator(
        task_id='validate_all_sources',
        python_callable=lambda **context: all([
            validate_schema({"source": "AWS"}, **context),
            validate_schema({"source": "Azure"}, **context),
            validate_schema({"source": "GCP"}, **context),
        ]),
    )
    
    # Transformación (usando módulo transformations)
    normalize = PythonOperator(
        task_id='normalize_data',
        python_callable=lambda **context: normalize_data(
            {"records": 1000}, {"records": 800}, {"records": 1200}, **context
        ),
    )
    
    enrich = PythonOperator(
        task_id='enrich_with_metadata',
        python_callable=lambda **context: enrich_with_metadata(
            {"normalized_records": 3000}, **context
        ),
    )
    
    end = EmptyOperator(task_id='end')
    
    # Dependencies
    start >> [connect_aws, connect_azure, connect_gcp]
    connect_aws >> extract_aws
    connect_azure >> extract_azure
    connect_gcp >> extract_gcp
    [extract_aws, extract_azure, extract_gcp] >> validate >> normalize >> enrich >> end
