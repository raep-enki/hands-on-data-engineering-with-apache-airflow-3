"""
Catchup - Estrategia Híbrida

Demuestra una estrategia común: iniciar con catchup=False
y luego usar backfill manual cuando sea necesario.

Este patrón es recomendado para la mayoría de los casos.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='catchup_hybrid_strategy',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,  # Comenzar sin catchup
    tags=['example', 'catchup']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    validate = BashOperator(
        task_id='validate_source_data',
        bash_command="""
        echo "✅ Validando que existan datos para {{ ds }}"
        echo "📂 Verificando: /data/{{ ds }}/"
        """
    )
    
    extract = BashOperator(
        task_id='extract_incremental',
        bash_command="""
        echo "📥 Extracción incremental para {{ ds }}"
        echo "🔄 Solo datos nuevos desde última ejecución"
        """
    )
    
    transform = BashOperator(
        task_id='transform_data',
        bash_command="""
        echo "⚙️ Transformando datos del {{ ds }}"
        echo "📊 Aplicando business rules"
        """
    )
    
    load = BashOperator(
        task_id='load_incremental',
        bash_command="""
        echo "📤 Carga incremental a warehouse"
        echo "💾 Tabla: daily_metrics_{{ ds_nodash }}"
        """
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> validate >> extract >> transform >> load >> end

dag.doc_md = """
# Estrategia Híbrida de Catchup

Este DAG usa la estrategia recomendada: **catchup=False + backfill manual**.

## Ventajas de esta estrategia:

### 1. Control del tiempo de activación
- DAG no ejecuta miles de runs al activarse
- Sistema permanece responsive
- Scheduler no se sobrecarga

### 2. Backfill selectivo
- Ejecutar backfill solo cuando realmente se necesite
- Elegir rangos de fechas específicos
- Control sobre recursos consumidos

### 3. Testing más fácil
- Activar DAG para probar sin generar runs históricos
- Desarrollo iterativo sin cleanup constante
- Debugging simplificado

## Cómo hacer backfill manual:

### Opción 1: CLI de Airflow
```bash
# Backfill para un rango específico
airflow dags backfill \\
    --start-date 2021-01-01 \\
    --end-date 2021-01-31 \\
    catchup_hybrid_strategy

# Backfill de un solo día
airflow dags backfill \\
    --start-date 2021-01-15 \\
    --end-date 2021-01-15 \\
    catchup_hybrid_strategy
```

### Opción 2: API REST
```python
import requests

response = requests.post(
    'http://airflow:8080/api/v1/dags/catchup_hybrid_strategy/dagRuns',
    json={
        'logical_date': '2021-01-15T00:00:00Z',
        'note': 'Backfill manual para procesar datos faltantes'
    }
)
```

### Opción 3: UI de Airflow
1. Ir al DAG en la UI
2. Click en "Trigger DAG w/ config"
3. Especificar logical_date manualmente

## Cuándo usar cada opción:

**catchup=True:**
- Pipeline de datos críticos que NO puede perder ningún intervalo
- Sistema legacy que espera procesamiento completo desde inicio
- Migraciones de datos donde necesitas todos los períodos

**catchup=False + backfill manual (recomendado):**
- Mayoría de los DAGs nuevos
- Desarrollo y testing
- Pipelines que pueden tolerar omitir períodos históricos
- Cuando quieres control explícito sobre procesamiento histórico
"""
