"""
HOMEWORK 15 - Mini Pipeline ETL Completo (Nivel: Muy Alto)
==========================================================

OBJETIVO:
Construir un pipeline ETL completo que integre todos los conceptos aprendidos.

REQUISITOS:
1. Crear un DAG llamado 'homework_15_etl_complete'
2. Configurar schedule='@daily', catchup=False

EXTRACTION PHASE (Task Group):
3. Crear TaskGroup 'extraction' con:
   - @task 'extract_customers': generar 50 clientes con {id, name, country, segment}
   - @task 'extract_orders': generar 200 órdenes con {id, customer_id, amount, date}
   - @task 'extract_products': generar 20 productos con {id, name, category, price}

VALIDATION PHASE:
4. Crear @task 'validate_data_quality' que:
   - Reciba las 3 extracciones
   - Valide:
     * No hay customer_ids en orders que no existen en customers
     * Todos los amounts son > 0
     * No hay productos con price = 0
   - Retorne validation_report: {'passed': bool, 'errors': []}
   
BRANCHING:
5. Crear @task.branch 'check_validation' que:
   - Si validation passed: retornar 'transformation'
   - Si falló: retornar 'data_quality_alert'

TRANSFORMATION PHASE (Task Group):
6. Crear TaskGroup 'transformation' con:
   - @task 'enrich_orders': join orders con customers y products
   - @task 'calculate_metrics': calcular revenue por país, por categoría
   - @task 'aggregate_daily': agregar métricas diarias

LOADING PHASE:
7. Crear @task 'load_to_warehouse' que:
   - Reciba datos transformados
   - Simule carga a warehouse
   - Retorne loading_stats

ERROR HANDLING:
8. Crear tarea 'data_quality_alert' para fallos de validación
9. Crear tarea 'success_notification' con trigger_rule.ALL_SUCCESS
10. Crear tarea 'failure_notification' con trigger_rule.ALL_FAILED
11. Crear tarea 'cleanup' con trigger_rule.ALL_DONE

MONITORING:
12. Agregar timeouts:
    - Extraction tasks: 5 minutos cada una
    - Transformation: 10 minutos
    - Loading: 5 minutos
13. Configurar retries: 2 intentos para todas las tareas

14. Agregar tags: ['homework', 'nivel_06', 'etl', 'final']

CONCEPTOS INTEGRADOS:
- TaskFlow API completa
- Task Groups para organización
- Validación de datos
- Branching condicional
- Error handling con trigger rules
- Setup/Teardown implícito
- Timeouts y retries
- Agregaciones y joins de datos
- Pipeline ETL end-to-end

RESULTADO ESPERADO:
Un pipeline ETL funcional que extrae, valida, transforma y carga datos,
con manejo robusto de errores y notificaciones.
"""

import datetime
import random
from datetime import timedelta

from airflow.sdk import DAG, task, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# TODO: Implementa tu solución aquí
# Este es el ejercicio final que integra todo lo aprendido
# Tómate tu tiempo y construye un pipeline completo y robusto
