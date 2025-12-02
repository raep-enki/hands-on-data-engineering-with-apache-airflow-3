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

**Ejemplo de módulo compartido (config/validators.py):**

```python
def validate_schema(data, expected_columns):
    """Valida que data tenga las columnas esperadas"""
    print(f"Validating schema: {expected_columns}")
    # Lógica de validación
    return True

def validate_nulls(data, columns):
    """Valida que columnas críticas no tengan nulls"""
    print(f"Checking nulls in: {columns}")
    return True

def validate_duplicates(data, key_columns):
    """Valida que no haya duplicados en key columns"""
    print(f"Checking duplicates on: {key_columns}")
    return True
```

**Ejemplo de módulo compartido (config/connectors.py):**

```python
def connect_to_aws(region='us-east-1'):
    """Conecta a AWS S3 en región especificada"""
    print(f"Connecting to AWS {region}")
    return f"aws_connection_{region}"

def connect_to_gcp(project='my-project'):
    """Conecta a GCP BigQuery en proyecto especificado"""
    print(f"Connecting to GCP {project}")
    return f"gcp_connection_{project}"
```

**Ejemplo de módulo compartido (config/transformers.py):**

```python
def normalize_dates(data):
    """Convierte fechas a formato ISO"""
    print("Normalizing dates to ISO format")
    return data

def convert_currency(data, from_currency, to_currency):
    """Convierte montos de una moneda a otra"""
    print(f"Converting {from_currency} to {to_currency}")
    return data
```

**El DAG multi_cloud_etl.py que usa los helpers:**

```python
import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Importar helpers compartidos
from config.validators import validate_schema, validate_nulls
from config.connectors import connect_to_aws, connect_to_gcp
from config.transformers import normalize_dates, convert_currency

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

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa el DAG con imports de módulos compartidos
