"""
HOMEWORK 10 - Tareas Dinámicas con Loop (Nivel: Medio-Alto)
===========================================================

OBJETIVO:
Generar tareas dinámicamente usando un loop (patrón pre-Airflow 2.3).

REQUISITOS:
1. Crear un DAG llamado 'homework_10_dynamic'
2. Definir una lista de ciudades:
   cities = ['New_York', 'London', 'Tokyo', 'Paris', 'Sydney']
3. Crear tarea 'start' (EmptyOperator)
4. Usar un for loop para crear dinámicamente:
   - Para cada ciudad, crear BashOperator con task_id=f'process_{city}'
   - bash_command que imprima "Procesando datos de {city}"
5. Crear tarea 'consolidate' (BashOperator) que:
   - Imprima "Consolidando datos de 5 ciudades"
6. Flujo: start >> [todas las tareas de ciudades en paralelo] >> consolidate
7. Configurar schedule='@daily', start_date enero 2021, catchup=False
8. Agregar tags: ['homework', 'nivel_04', 'dynamic']

CONCEPTOS:
- Generación dinámica de tareas con loops
- DAG con estructura variable
- Patrón para procesar múltiples items
- Dependencias dinámicas

PISTA:
cities = ['A', 'B', 'C']
tasks = []
for city in cities:
    t = BashOperator(task_id=f'process_{city}', ...)
    tasks.append(t)

start >> tasks >> end

RESULTADO ESPERADO:
DAG que crea automáticamente una tarea por cada ciudad.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa tu solución aquí
