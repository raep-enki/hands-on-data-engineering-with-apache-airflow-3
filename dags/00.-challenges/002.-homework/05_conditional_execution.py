"""
HOMEWORK 05 - Ejecución Condicional (Nivel: Fácil)
==================================================

OBJETIVO:
Implementar branching para ejecutar diferentes rutas según una condición.

REQUISITOS:
1. Crear un DAG llamado 'homework_05_conditional'
2. Crear una tarea 'start' (EmptyOperator)
3. Crear una función @task.branch llamada 'check_day_of_week' que:
   - Use context['logical_date'] para obtener la fecha
   - Si es Lunes (weekday == 0): retornar 'monday_task'
   - Si es Viernes (weekday == 4): retornar 'friday_task'
   - Cualquier otro día: retornar 'midweek_task'
4. Crear 3 tareas con BashOperator:
   - 'monday_task': "🌅 Inicio de semana - Plan semanal"
   - 'friday_task': "🎉 Fin de semana - Reporte semanal"
   - 'midweek_task': "⚙️ Día regular - Procesamiento estándar"
5. Crear tarea 'end' (EmptyOperator) con trigger_rule=TriggerRule.NONE_FAILED
6. Flujo: start >> check >> [3 opciones] >> end
7. Configurar schedule='@daily', start_date enero 2021, catchup=False
8. Agregar tags: ['homework', 'nivel_02', 'branching']

CONCEPTOS:
- BranchPythonOperator / @task.branch
- Lógica condicional en DAGs
- logical_date y weekday
- TriggerRule.NONE_FAILED

PISTA:
from airflow.sdk import TriggerRule

@task.branch(task_id='check_day')
def check_condition(**context):
    date = context['logical_date']
    if date.weekday() == 0:
        return 'task_a'
    return 'task_b'

RESULTADO ESPERADO:
Dependiendo del día de la semana, se ejecuta una ruta diferente.
"""

import datetime

from airflow.sdk import DAG, task, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa tu solución aquí
