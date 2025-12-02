"""
HOMEWORK 09 - Organización con Task Groups (Nivel: Medio)
=========================================================

OBJETIVO:
Organizar un DAG complejo usando Task Groups para mejor visualización.

REQUISITOS:
1. Crear un DAG llamado 'homework_09_task_groups'
2. Crear tarea 'start' (EmptyOperator)
3. Crear Task Group 'data_extraction' con 3 tareas:
   - 'extract_database': BashOperator "Extrayendo de PostgreSQL"
   - 'extract_api': BashOperator "Extrayendo de REST API"
   - 'extract_files': BashOperator "Extrayendo CSV files"
   (Las 3 en paralelo dentro del grupo)
4. Crear Task Group 'data_processing' con 2 tareas secuenciales:
   - 'clean_data': BashOperator "Limpiando datos"
   - 'transform_data': BashOperator "Transformando datos"
5. Crear Task Group 'data_quality' con 2 tareas paralelas:
   - 'validate_schema': BashOperator "Validando esquema"
   - 'check_duplicates': BashOperator "Chequeando duplicados"
6. Crear tarea 'end' (EmptyOperator)
7. Flujo: start >> data_extraction >> data_processing >> data_quality >> end
8. Configurar schedule='@daily', start_date enero 2021, catchup=False
9. Agregar tags: ['homework', 'nivel_03', 'task_groups']

CONCEPTOS:
- TaskGroup para organización lógica
- Jerarquía visual en el DAG
- Grupos anidados de tareas
- Patrón ETL organizado

PISTA:
from airflow.utils.task_group import TaskGroup

with TaskGroup(group_id='my_group') as group:
    task1 = BashOperator(...)
    task2 = BashOperator(...)

RESULTADO ESPERADO:
DAG organizado en grupos lógicos que mejoran la comprensión del flujo.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# TODO: Implementa tu solución aquí
