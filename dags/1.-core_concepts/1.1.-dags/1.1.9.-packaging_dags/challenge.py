"""
DESAFÍO: Sistema de DAGs con Arquitectura Modular

Crea un sistema de 3 DAGs relacionados que demuestren buenas prácticas
de packaging, modularidad y reutilización de código.

CONTEXTO:
Tienes un sistema de analytics que procesa datos de múltiples fuentes:
- Transacciones de ventas (alta frecuencia, alta prioridad)
- Datos de inventario (media frecuencia, media prioridad)
- Logs de comportamiento (alta frecuencia, baja prioridad)

REQUISITOS:

1. Configuración Compartida:
   - Definir COMMON_DEFAULT_ARGS aplicables a todos los DAGs
   - Definir DATA_SOURCES con configuración de cada fuente
   - Definir PROCESSING_CONFIG con parámetros de procesamiento
   - Incluir al menos 5 configuraciones compartidas

2. Funciones Helper Compartidas:
   - validate_data_quality(source_name, **context): Validación genérica
   - send_notification(status, dag_id, **context): Notificaciones
   - log_metrics(metrics_dict, **context): Logging de métricas
   - Cada función debe imprimir información descriptiva

3. Factory Function:
   - create_analytics_dag(source_name, schedule, priority)
   - Debe crear DAGs con estructura consistente
   - Ajustar retries según priority (high=3, medium=2, low=1)
   - Ajustar timeout según priority

4. DAG 1: Sales Analytics (Alta Prioridad)
   - dag_id: 'packaging_challenge_sales'
   - schedule: '@hourly'
   - priority: 'high'
   - Pipeline: extract -> validate -> transform -> enrich -> load
   - Usar funciones helper compartidas

5. DAG 2: Inventory Analytics (Media Prioridad)
   - dag_id: 'packaging_challenge_inventory'
   - schedule: '0 */4 * * *'  # cada 4 horas
   - priority: 'medium'
   - Pipeline: extract -> validate -> transform -> reconcile -> load
   - Usar funciones helper compartidas

6. DAG 3: Behavior Logs (Baja Prioridad)
   - dag_id: 'packaging_challenge_behavior'
   - schedule: '0 1 * * *'  # 1 AM diario
   - priority: 'low'
   - Pipeline: extract -> filter -> aggregate -> load
   - Usar funciones helper compartidas

7. Notificaciones:
   - Todos los DAGs deben notificar al iniciar
   - Todos los DAGs deben notificar al completar
   - Usar la función send_notification compartida

8. Métricas:
   - Cada DAG debe loggear métricas al finalizar
   - Métricas: records_processed, execution_time, quality_score
   - Usar la función log_metrics compartida

9. Tags Estandarizados:
   - Todos deben incluir: 'example', 'core_concepts', 'dags', 'packaging_dags'
   - Agregar tag de priority
   - Agregar tag de source_name

RESTRICCIONES:
- Usar ÚNICAMENTE: BashOperator, PythonOperator, EmptyOperator
- NO duplicar código entre DAGs
- Toda la configuración debe estar al inicio del archivo
- Las funciones helper deben ser reutilizables
- Debe haber exactamente 3 DAGs creados
- Cada DAG mínimo 8 tareas

ESTRUCTURA ESPERADA:
```python
# Configuración compartida
COMMON_DEFAULT_ARGS = {...}
DATA_SOURCES = {...}
PROCESSING_CONFIG = {...}

# Funciones helper
def validate_data_quality(...): ...
def send_notification(...): ...
def log_metrics(...): ...

# Factory function
def create_analytics_dag(...): ...

# Crear los 3 DAGs
sales_dag = create_analytics_dag(...)
inventory_dag = create_analytics_dag(...)
behavior_dag = create_analytics_dag(...)
```

TIPS:
- Usa op_kwargs para pasar parámetros a las funciones Python
- Las funciones helper deben recibir **context
- Ajusta bash_command según el source_name y stage
- Los DAGs deben ser funcionalmente diferentes pero estructuralmente similares
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Define configuración compartida

# TODO: Define funciones helper

# TODO: Define factory function

# TODO: Crea los 3 DAGs
