"""
HOMEWORK 06 - Validación de Datos (Nivel: Medio)
================================================

OBJETIVO:
Implementar un pipeline con validación de datos y manejo de errores.

REQUISITOS:
1. Crear un DAG llamado 'homework_06_validation'
2. Crear función @task 'extract_data' que retorne un dict:
   {
       'records': [
           {'id': 1, 'value': 100, 'status': 'valid'},
           {'id': 2, 'value': -50, 'status': 'valid'},
           {'id': 3, 'value': 200, 'status': 'valid'},
           {'id': 4, 'value': 0, 'status': 'valid'}
       ]
   }
3. Crear función @task 'validate_data' que:
   - Reciba los datos
   - Marque como 'invalid' registros con value <= 0
   - Cuente válidos e inválidos
   - Imprima resumen
   - Retorne datos actualizados + conteo
4. Crear función @task 'process_valid_data' que:
   - Reciba los datos validados
   - Procese SOLO los registros válidos
   - Sume los valores válidos
   - Imprima "Total procesado: $X"
5. Crear función @task 'quarantine_invalid_data' que:
   - Reciba los datos validados
   - Imprima los registros inválidos en "cuarentena"
6. Flujo: extract >> validate >> [process_valid, quarantine_invalid]
7. Configurar schedule='@daily', start_date enero 2021, catchup=False
8. Agregar tags: ['homework', 'nivel_03', 'validation']

CONCEPTOS:
- Validación de datos
- Procesamiento condicional de registros
- Múltiples outputs desde una tarea
- Manejo de datos inválidos (quarantine pattern)

RESULTADO ESPERADO:
Pipeline que identifica datos inválidos y los procesa por separado.
"""

import datetime

from airflow.sdk import DAG, task

# TODO: Implementa tu solución aquí
