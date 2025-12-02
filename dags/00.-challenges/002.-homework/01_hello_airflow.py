"""
HOMEWORK 01 - Hello Airflow (Nivel: Muy Fácil)
==============================================

OBJETIVO:
Crear tu primer DAG funcional que imprima mensajes en 3 tareas secuenciales.

REQUISITOS:
1. Crear un DAG llamado 'homework_01_hello'
2. Agregar 3 tareas usando BashOperator con los siguientes mensajes:
   - Tarea 1: "🚀 Iniciando mi primer DAG de homework"
   - Tarea 2: "⚙️ Procesando datos..."
   - Tarea 3: "✅ DAG completado exitosamente"
3. Las tareas deben ejecutarse en secuencia (una después de la otra)
4. Configurar schedule='@daily' y start_date en enero 2021
5. Desactivar catchup
6. Agregar tags: ['homework', 'nivel_01', 'basico']

CONCEPTOS:
- Declaración básica de DAG
- BashOperator
- Dependencias secuenciales simples (>>)
- Configuración básica (schedule, start_date, catchup)

PISTA:
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

RESULTADO ESPERADO:
Un DAG que cuando se ejecute muestre los 3 mensajes en orden.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# TODO: Implementa tu solución aquí
# Crea el DAG con las especificaciones indicadas
