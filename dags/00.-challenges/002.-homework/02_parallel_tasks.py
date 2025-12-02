"""
HOMEWORK 02 - Tareas en Paralelo (Nivel: Muy Fácil)
===================================================

OBJETIVO:
Crear un DAG con tareas que se ejecuten en paralelo.

REQUISITOS:
1. Crear un DAG llamado 'homework_02_parallel'
2. Crear una tarea inicial llamada 'start' (usar EmptyOperator)
3. Crear 4 tareas paralelas que simulen procesar diferentes fuentes de datos:
   - 'process_database': "Procesando datos de base de datos"
   - 'process_api': "Procesando datos de API"
   - 'process_files': "Procesando archivos CSV"
   - 'process_logs': "Procesando logs del sistema"
4. Crear una tarea final llamada 'end' (usar EmptyOperator)
5. Flujo: start >> [las 4 tareas paralelas] >> end
6. Configurar schedule='@daily', start_date enero 2021, catchup=False
7. Agregar tags: ['homework', 'nivel_01', 'basico']

CONCEPTOS:
- EmptyOperator (para tareas placeholder)
- Dependencias paralelas usando listas
- Patrón fan-out / fan-in

PISTA:
Para ejecutar tareas en paralelo:
start >> [task1, task2, task3, task4] >> end

RESULTADO ESPERADO:
Las 4 tareas de procesamiento deben ejecutarse simultáneamente después de 'start'.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa tu solución aquí
