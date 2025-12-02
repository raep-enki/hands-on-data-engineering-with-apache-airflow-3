"""
HOMEWORK 03 - Reporte Diario con Templating (Nivel: Fácil)
==========================================================

OBJETIVO:
Crear un DAG que genere un reporte diario usando templates de Jinja2.

REQUISITOS:
1. Crear un DAG llamado 'homework_03_daily_report'
2. Crear 3 tareas con BashOperator:
   - 'extract_data': Mostrar "Extrayendo datos del {{ ds }}"
   - 'generate_report': Mostrar "Generando reporte para {{ ds }}" y dormir 2 segundos
   - 'send_notification': Mostrar "✉️ Reporte de {{ ds }} enviado exitosamente"
3. Ejecutar en secuencia
4. Configurar schedule='@daily', start_date enero 2021, catchup=False
5. Agregar tags: ['homework', 'nivel_01', 'templating']

CONCEPTOS:
- Jinja2 templating en Airflow
- Variable {{ ds }} (execution date en formato YYYY-MM-DD)
- Templates en bash_command

PISTA:
bash_command="echo 'Fecha: {{ ds }}'"

RESULTADO ESPERADO:
Al ejecutar el DAG, cada tarea debe mostrar la fecha de ejecución.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# TODO: Implementa tu solución aquí
