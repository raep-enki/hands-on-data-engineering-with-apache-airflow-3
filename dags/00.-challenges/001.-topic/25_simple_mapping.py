"""
Challenge: Procesar N Fuentes Sin Saber Cuántas Son

Tienes un pipeline que extrae de S3, GCS, Azure Blob, y FTP. Mañana te piden agregar SFTP y HTTP.
Podrías crear una tarea por fuente (extract_s3, extract_gcs, etc.), pero cada vez que agregan
una fuente, debes modificar el DAG.

La solución: Dynamic Task Mapping. Una tarea genera la lista de fuentes, y otra tarea se "mapea"
sobre esa lista, creando una instancia por cada fuente automáticamente. Es como un `for` loop,
pero cada iteración es una tarea paralela.

La lista puede cambiar dinámicamente. Hoy 4 fuentes, mañana 6. El DAG se adapta automáticamente.
Además, puedes mapear en cadena: extraer se mapea por fuentes, validar se mapea por resultados
de extracciones.

Crea el DAG `simple_mapping_challenge` diario, sin catchup, usando `.expand()` para mapear tareas
dinámicamente sobre listas. Tags: `['challenge', 'simple_mapping']`.
"""

import datetime

from airflow.sdk import DAG, task

# TODO: Usa @task con .expand() para dynamic task mapping
