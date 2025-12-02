"""
Challenge: El Modelo de ML Que Deja Recursos Sin Limpiar

Tu equipo entrena modelos de ML semanalmente, pero hay un problema: si algo falla a mitad del proceso,
quedan directorios temporales ocupando espacio (200GB+ de basura), conexiones a bases de datos abiertas
(agota el connection pool), y GPUs reservadas que nadie libera ($50/hora desperdiciado).

Necesitas implementar el pattern setup/teardown de Airflow 3.x: tareas que preparan recursos y
GARANTIZAN su limpieza, falle o no el proceso principal. Es como try-finally pero declarativo.

**El flujo con setup/teardown:**

`start` (EmptyOperator) se ramifica a 3 setups en paralelo:

**Setups (preparan recursos):**
- `setup_temp_directories` (BashOperator - crea /tmp/ml_training/, /tmp/datasets/, /tmp/models/)
- `setup_database_connection` (BashOperator - abre connection pool a PostgreSQL, max 10 connections)
- `setup_gpu_allocation` (BashOperator - reserva 2x NVIDIA A100 en cloud)

**Work tasks (dependen de setups):**
`[todos los setups]` >> `validate_prerequisites` (BashOperator - verifica Python packages instalados,
libcuda disponible, espacio en disco suficiente) >>

`download_training_data` (BashOperator - descarga 50GB desde S3 a /tmp/datasets/) >>

`split_train_test` (BashOperator - divide en train 80% / test 20%) >>

Se ramifica en 2 validaciones paralelas:
- `validate_training_data` (BashOperator - verifica formato, missing values, outliers)
- `validate_test_data` (BashOperator - verifica misma estructura que train)

`[ambas validaciones]` >> `train_model` (BashOperator - entrena XGBoost en GPU, puede tardar 2hrs) >>

`evaluate_model` (BashOperator - calcula accuracy, precision, recall en test set) >>

`branch_by_performance` (BranchPythonOperator - decide según accuracy):
- Si accuracy >= 0.85 >> `deploy_model_to_staging` (BashOperator - copia modelo a S3)
- Si accuracy < 0.85 >> `notify_poor_performance` (BashOperator - envía alerta a Slack)

**Teardowns (limpian recursos SIEMPRE, con .as_teardown()):**

Cada setup tiene su teardown correspondiente usando `.as_teardown(setups=...)`:

`cleanup_temp_directories` (BashOperator - borra /tmp/ml_training/, libera 200GB).as_teardown(setups=setup_temp_directories)
`cleanup_database_connection` (BashOperator - cierra connections, libera pool).as_teardown(setups=setup_database_connection)
`cleanup_gpu_allocation` (BashOperator - libera GPUs, detiene instancias cloud).as_teardown(setups=setup_gpu_allocation)

**Flujo de dependencies:**
Los teardowns se ejecutan automáticamente después del último work task que depende de su setup,
sin importar si hubo éxito o falla. Así garantizas limpieza.

`[deploy_model_to_staging, notify_poor_performance]` >> [todos los teardowns] >> `end` (EmptyOperator)

**Patrón clave:**
```python
s1 = BashOperator(task_id='setup_temp_directories', ...)
work = BashOperator(task_id='train_model', ...)
t1 = BashOperator(task_id='cleanup_temp_directories', ...).as_teardown(setups=s1)

s1 >> work >> t1  # t1 se ejecuta SIEMPRE después de work
```

**Configuración técnica:**
- DAG ID: `ml_training_pipeline`
- Schedule: @weekly (cada domingo a las 00:00)
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'setup_and_teardown']`
- 3 pares setup/teardown (total 6 tareas), más 10+ work tasks
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa el DAG según los requisitos
