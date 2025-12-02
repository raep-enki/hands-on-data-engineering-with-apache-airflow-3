"""
HOMEWORK 07 - Patrón de Reintentos (Nivel: Medio)
=================================================

OBJETIVO:
Configurar reintentos para tareas que pueden fallar temporalmente.

REQUISITOS:
1. Crear un DAG llamado 'homework_07_retry'
2. Usar default_args para configurar reintentos:
   - retries: 3
   - retry_delay: timedelta(minutes=2)
3. Crear función @task 'fetch_api_data' que:
   - Use random.random() para simular fallo
   - Si random < 0.4 (40% probabilidad): lanzar Exception("API timeout")
   - Si random >= 0.4: retornar {'status': 'success', 'data': 'API data'}
   - Imprimir intento actual
4. Crear función @task 'process_data' que:
   - Reciba los datos
   - Imprima "✅ Datos procesados: {data}"
5. Crear tarea BashOperator 'critical_task' con:
   - retries: 5 (override del default)
   - bash_command que simule fallo aleatorio con $RANDOM
6. Flujo: fetch_api_data >> process_data >> critical_task
7. Configurar schedule='@daily', start_date enero 2021, catchup=False
8. Agregar tags: ['homework', 'nivel_03', 'retry']

CONCEPTOS:
- default_args en DAG
- Configuración de reintentos (retries, retry_delay)
- Override de configuración a nivel de tarea
- Manejo de fallos temporales
- try_number en contexto

PISTA:
import random
from datetime import timedelta

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

RESULTADO ESPERADO:
Si una tarea falla, se reintenta automáticamente según configuración.
"""

import datetime
import random
from datetime import timedelta

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

# TODO: Implementa tu solución aquí
