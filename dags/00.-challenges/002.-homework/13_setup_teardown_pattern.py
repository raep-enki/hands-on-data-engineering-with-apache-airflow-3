"""
HOMEWORK 13 - Setup y Teardown Resources (Nivel: Alto)
======================================================

OBJETIVO:
Implementar el patrón setup/teardown para manejar recursos que necesitan
inicialización y limpieza.

REQUISITOS:
1. Crear un DAG llamado 'homework_13_setup_teardown'
2. Crear tarea 'setup_database' (BashOperator) que:
   - Imprima "🔧 Creando conexión temporal a base de datos"
   - Imprima "🔧 Creando tabla staging: staging_data_YYYYMMDD"
   - Use {{ ds_nodash }} en el nombre de la tabla
3. Crear tarea 'setup_api_connection' (BashOperator) que:
   - Imprima "🔌 Estableciendo conexión con API externa"
   - Imprima "🔌 Obteniendo token de autenticación"
4. Crear función @task 'load_data' que:
   - Simule carga de 1000 registros
   - Imprima progreso
   - Retorne {'records_loaded': 1000}
5. Crear función @task 'process_data' que:
   - Reciba result de load_data
   - Simule procesamiento
   - Retorne {'records_processed': 1000}
6. Crear tarea 'teardown_database' (BashOperator) que:
   - Imprima "🧹 Eliminando tabla staging"
   - Imprima "🧹 Cerrando conexión a base de datos"
   - Configure .as_teardown(setups=setup_database)
7. Crear tarea 'teardown_api' (BashOperator) que:
   - Imprima "🔌 Cerrando conexión API"
   - Configure .as_teardown(setups=setup_api_connection)
8. Flujo:
   setup_database >> load_data >> process_data >> teardown_database
   setup_api_connection >> load_data
   setup_api_connection >> teardown_api
9. Configurar schedule='@daily', start_date enero 2021, catchup=False
10. Agregar tags: ['homework', 'nivel_05', 'setup_teardown']

CONCEPTOS:
- Setup/Teardown pattern
- Resource management
- Garantía de limpieza incluso con fallos
- .as_teardown() method

RESULTADO ESPERADO:
Los recursos se limpian automáticamente, incluso si el procesamiento falla.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

# TODO: Implementa tu solución aquí
