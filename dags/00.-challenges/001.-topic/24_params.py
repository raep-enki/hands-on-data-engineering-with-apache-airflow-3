"""
Challenge: Triggerear el DAG Con Parámetros Custom

Hoy quieres procesar la última semana de datos. Mañana del 1-15 de noviembre. Pasado extraer de
Salesforce, luego de HubSpot. A veces incluir registros borrados, a veces no. A veces batch_size
1000, a veces 5000. A veces modo "full_refresh", a veces "incremental".

Son decisiones que tomas cuando ejecutas el DAG, no cuando lo escribes. **Params** te permite:
- Definir parámetros que el DAG acepta
- Validaciones automáticas (tipos, rangos, valores permitidos)
- Valores default
- Descripciones para la UI

Cuando triggeas desde UI o CLI, pasas valores. Airflow valida y rechaza si no cumplen las reglas.

**IMPORTANTE: Todas las tareas usan PythonOperator (@task)** porque necesitan:
- Acceder a params del DAG: `context['params']['param_name']`
- Lógica condicional basada en params (if start_date, if source_system)
- Validaciones custom de parámetros
- Branching según processing_mode
- Procesamiento adaptativo según batch_size

BashOperator no puede acceder a params fácilmente. Usa @task para lógica parameterizada.

**Params que debe aceptar el DAG:**

```python
params={
    'start_date': Param(
        default='2024-01-01',
        type='string',
        pattern=r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        description='Start date for processing (YYYY-MM-DD)'
    ),
    'end_date': Param(
        default='2024-01-07',
        type='string',
        pattern=r'^\d{4}-\d{2}-\d{2}$',
        description='End date for processing (YYYY-MM-DD)'
    ),
    'source_system': Param(
        default='salesforce',
        type='string',
        enum=['salesforce', 'hubspot', 'marketo', 'database'],
        description='Source system to extract from'
    ),
    'processing_mode': Param(
        default='incremental',
        type='string',
        enum=['incremental', 'full_refresh'],
        description='Processing strategy'
    ),
    'batch_size': Param(
        default=1000,
        type='integer',
        minimum=100,
        maximum=10000,
        description='Number of records per batch'
    ),
    'include_deleted': Param(
        default=False,
        type='boolean',
        description='Include soft-deleted records'
    ),
    'enable_validation': Param(
        default=True,
        type='boolean',
        description='Run data quality validations'
    )
}
```

**El pipeline parameterizado:**

Crea 8 tareas @task que usen los params:
1. **validate_params** (PythonOperator): Valida fechas, source_system existe, batch_size razonable
2. **extract_from_source** (PythonOperator): Extrae según source_system param, usa date range
3. **filter_records** (PythonOperator): Filtra deleted si include_deleted=False
4. **branch_by_mode** (BranchPythonOperator): Decide path según processing_mode param
5. **process_incremental** (PythonOperator): Solo procesa nuevos, usa batch_size param
6. **process_full_refresh** (PythonOperator): Reemplaza todo
7. **run_validations** (PythonOperator): Solo si enable_validation=True
8. **generate_summary** (PythonOperator): Reporte con todos los params usados

**Cómo triggear con params:**

Desde UI: Trigger DAG → "Trigger DAG w/ config" → JSON:
```json
{
  "start_date": "2024-11-01",
  "end_date": "2024-11-15",
  "source_system": "hubspot",
  "processing_mode": "full_refresh",
  "batch_size": 5000,
  "include_deleted": true,
  "enable_validation": false
}
```

Desde CLI:
```bash
airflow dags trigger params_challenge \
  --conf '{"source_system":"salesforce","batch_size":2000}'
```

**Configuración técnica:**
- DAG ID: `params_challenge`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'params']`
- 7 Params definidos con validaciones
- 8 tareas @task que leen y usan params
- Branching basado en params
- Usa PythonOperator (@task) para TODAS las tareas (acceso a params)
"""

import datetime

from airflow.sdk import DAG, task, Param

# TODO: Define Params con validaciones y úsalos en @task
