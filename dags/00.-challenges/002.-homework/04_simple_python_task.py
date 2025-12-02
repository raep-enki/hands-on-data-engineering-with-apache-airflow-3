"""
HOMEWORK 04 - Primera Tarea Python (Nivel: Fácil)
=================================================

OBJETIVO:
Usar TaskFlow API para crear tareas Python que procesen datos.

REQUISITOS:
1. Crear un DAG llamado 'homework_04_python_task'
2. Crear una función decorada con @task que:
   - Se llame 'calculate_metrics'
   - Calcule las siguientes métricas:
     * total_users = 15420
     * active_users = 8934
     * conversion_rate = active_users / total_users
   - Imprima las métricas con formato bonito
   - Retorne un dict con las tres métricas
3. Crear una segunda función @task que:
   - Se llame 'send_metrics'
   - Reciba el dict de métricas como parámetro
   - Imprima "📊 Enviando métricas al dashboard..."
   - Imprima cada métrica
4. Conectar: calculate_metrics() >> send_metrics()
5. Configurar schedule='@daily', start_date enero 2021, catchup=False
6. Agregar tags: ['homework', 'nivel_02', 'taskflow']

CONCEPTOS:
- TaskFlow API con decorador @task
- Funciones Python en Airflow
- Paso de datos entre tareas (XCom automático)
- Return values

PISTA:
@task
def my_function():
    result = {'key': 'value'}
    return result

RESULTADO ESPERADO:
La primera tarea calcula métricas y la segunda las recibe y muestra.
"""

import datetime

from airflow.sdk import DAG, task

# TODO: Implementa tu solución aquí
