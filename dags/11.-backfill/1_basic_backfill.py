"""
Backfill - Concepto Básico

Demuestra qué es backfill y cuándo se usa.
Backfill es el proceso de ejecutar DAG runs para períodos
de tiempo históricos, típicamente después de que el DAG
ya fue creado o modificado.

Este DAG tiene catchup=False, por lo que backfill debe hacerse manualmente.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='backfill_basic_concept',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,  # Backfill será manual
    tags=['example', 'backfill']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    process = BashOperator(
        task_id='process_historical',
        bash_command="""
        echo "📅 Procesando datos históricos para: {{ ds }}"
        echo "🔄 Este run fue creado por backfill manual"
        """
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> process >> end

dag.doc_md = """
# Backfill - Concepto Básico

**¿Qué es backfill?**
Backfill es ejecutar un DAG para períodos de tiempo pasados que
no fueron procesados originalmente.

## Cuándo necesitas backfill:

### 1. DAG nuevo con catchup=False
- Creaste un DAG hoy pero necesitas datos desde hace 3 meses
- Solución: Hacer backfill del rango de fechas necesario

### 2. DAG estuvo pausado
- El DAG estuvo desactivado por 2 semanas
- Al reactivarlo, no ejecuta esos días automáticamente (si catchup=False)
- Solución: Backfill de las 2 semanas perdidas

### 3. Fix de bugs en lógica
- Descubriste un bug en la transformación de datos
- Corregiste el código
- Necesitas reprocesar datos históricos con la lógica corregida
- Solución: Backfill del rango afectado

### 4. Datos llegaron tarde
- Esperabas datos para el 15 de enero
- Los datos llegaron el 20 de enero
- Solución: Backfill del 15 de enero con datos completos

### 5. Migración de sistemas
- Migrando de otro scheduler a Airflow
- Necesitas procesar datos históricos en el nuevo sistema
- Solución: Backfill de todo el rango histórico

## Cómo hacer backfill:

### Usando CLI (recomendado para rangos grandes):
```bash
# Backfill de un rango de fechas
airflow dags backfill \\
    --start-date 2021-01-01 \\
    --end-date 2021-01-31 \\
    backfill_basic_concept

# Backfill con rerun (sobrescribir runs existentes)
airflow dags backfill \\
    --start-date 2021-01-01 \\
    --end-date 2021-01-31 \\
    --rerun-failed-tasks \\
    backfill_basic_concept
```

### Usando UI (recomendado para fechas individuales):
1. Navegar al DAG en la UI
2. Click en "Trigger DAG w/ config"
3. Especificar la logical_date deseada

## Diferencia con catchup:
- **catchup=True**: Backfill automático al activar DAG
- **catchup=False + backfill manual**: Control explícito de qué reprocesar
"""
