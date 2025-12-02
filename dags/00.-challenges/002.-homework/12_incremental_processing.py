"""
HOMEWORK 12 - Procesamiento Incremental con Dates (Nivel: Alto)
===============================================================

OBJETIVO:
Implementar procesamiento incremental usando logical_date para procesar 
datos por período.

REQUISITOS:
1. Crear un DAG llamado 'homework_12_incremental'
2. Configurar schedule='@hourly' (procesar cada hora)
3. Crear función @task 'get_processing_window' que:
   - Use context['logical_date'] para obtener la fecha/hora lógica
   - Calcule start_time = logical_date
   - Calcule end_time = logical_date + timedelta(hours=1)
   - Retorne {'start': start_time, 'end': end_time}
   - Imprima ventana de procesamiento
4. Crear función @task 'extract_incremental_data' que:
   - Reciba window dict
   - Simule extracción de datos para ese período
   - Genere 10-20 registros random con timestamps en la ventana
   - Cada registro: {'id': X, 'timestamp': T, 'value': V}
   - Retorne lista de registros
5. Crear función @task 'process_batch' que:
   - Reciba los registros
   - Calcule estadísticas (count, sum, avg)
   - Imprima resumen del batch
6. Crear función @task 'update_watermark' que:
   - Reciba window
   - Imprima "✅ Watermark actualizado: {end_time}"
   - Simule persistencia del watermark
7. Flujo: get_window >> extract >> process >> update_watermark
8. start_date enero 2021, catchup=False
9. Agregar tags: ['homework', 'nivel_05', 'incremental']

CONCEPTOS:
- Procesamiento incremental
- logical_date para ventanas de tiempo
- Watermarking pattern
- Schedule hourly
- Cálculo de períodos de tiempo

RESULTADO ESPERADO:
DAG que procesa datos por hora, manteniendo control de lo procesado.
"""

import datetime
import random
from datetime import timedelta

from airflow.sdk import DAG, task

# TODO: Implementa tu solución aquí
