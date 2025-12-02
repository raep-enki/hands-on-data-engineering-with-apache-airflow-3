"""
HOMEWORK 14 - Branching Complejo Multi-nivel (Nivel: Alto)
==========================================================

OBJETIVO:
Implementar branching complejo con múltiples niveles de decisión.

REQUISITOS:
1. Crear un DAG llamado 'homework_14_complex_branching'
2. Crear función @task 'get_data_size' que:
   - Genere tamaño random entre 100 y 10000
   - Retorne {'size': size}
3. Crear función @task.branch 'classify_by_size' que:
   - Reciba size
   - Si size < 1000: retornar 'small_dataset_branch'
   - Si 1000 <= size < 5000: retornar 'medium_dataset_branch'
   - Si size >= 5000: retornar 'large_dataset_branch'
4. Crear función @task.branch 'check_priority_small' que:
   - Use random para decidir prioridad
   - Retornar 'quick_process' o 'standard_process'
5. Crear función @task.branch 'check_priority_medium' que:
   - Similar a small pero retornar 'parallel_process' o 'standard_process'
6. Crear función @task 'large_dataset_process' que:
   - Imprima "🏋️ Procesamiento masivo paralelo"
7. Crear las tareas correspondientes para cada rama
8. Todas las ramas deben converger en tarea 'end' con trigger_rule adecuado
9. Flujo:
   get_size >> classify >> [small_branch, medium_branch, large_branch]
   small_branch >> check_priority >> [quick, standard]
   medium_branch >> check_priority >> [parallel, standard]
   large_branch >> large_dataset_process
   todos convergen en >> end
10. Configurar schedule='@daily', start_date enero 2021, catchup=False
11. Agregar tags: ['homework', 'nivel_05', 'complex_branching']

CONCEPTOS:
- Branching multi-nivel
- Decisiones encadenadas
- Múltiples rutas de ejecución
- Convergencia con trigger rules

RESULTADO ESPERADO:
Pipeline que toma decisiones en múltiples niveles según características de datos.
"""

import datetime
import random

from airflow.sdk import DAG, task, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa tu solución aquí
