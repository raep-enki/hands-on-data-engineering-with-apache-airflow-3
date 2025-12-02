"""
HOMEWORK 11 - Manejo de Errores con Trigger Rules (Nivel: Medio-Alto)
======================================================================

OBJETIVO:
Implementar un pipeline robusto con manejo de errores usando trigger rules.

REQUISITOS:
1. Crear un DAG llamado 'homework_11_error_handling'
2. Crear tarea 'start' (EmptyOperator)
3. Crear función @task 'risky_operation' que:
   - Use random.random()
   - Si random < 0.5: lanzar Exception("Operation failed!")
   - Si random >= 0.5: retornar {'status': 'success'}
4. Crear tarea 'success_notification' (BashOperator) con:
   - trigger_rule=TriggerRule.ALL_SUCCESS
   - bash_command="echo '✅ Operación exitosa - Enviando notificación'"
5. Crear tarea 'failure_notification' (BashOperator) con:
   - trigger_rule=TriggerRule.ALL_FAILED
   - bash_command="echo '⚠️ Operación falló - Enviando alerta al equipo'"
6. Crear tarea 'cleanup' (BashOperator) con:
   - trigger_rule=TriggerRule.ALL_DONE
   - bash_command="echo '🧹 Limpieza de recursos temporales'"
7. Crear tarea 'end' (EmptyOperator) con:
   - trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
8. Flujo:
   start >> risky_operation >> [success_notification, failure_notification]
   [success_notification, failure_notification] >> cleanup >> end
9. Configurar schedule='@daily', start_date enero 2021, catchup=False
10. Agregar tags: ['homework', 'nivel_04', 'error_handling']

CONCEPTOS:
- Trigger Rules (ALL_SUCCESS, ALL_FAILED, ALL_DONE, NONE_FAILED_MIN_ONE_SUCCESS)
- Manejo de errores en pipelines
- Notificaciones condicionales
- Cleanup tasks que siempre ejecutan

RESULTADO ESPERADO:
Pipeline que maneja exitosamente tanto éxito como fallos.
"""

import datetime
import random

from airflow.sdk import DAG, task, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa tu solución aquí
