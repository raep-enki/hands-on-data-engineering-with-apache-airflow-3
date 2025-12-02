"""
Challenge: Pipelines Que Se Disparan Por Datos, No Por Tiempo

Normalmente los DAGs corren por schedule: "cada hora", "todos los días a las 2 AM". Pero a veces
quieres que un DAG corra cuando OTRO DAG termine de generar datos. No es tiempo, es dependencia
de datos.

Ejemplo: un DAG procesa ventas crudas y genera "processed_sales.parquet". Otro DAG debe correr
CUANDO ese archivo esté listo, no a una hora específica. Es event-driven, no time-driven.

Assets (antes llamados Datasets) permiten esto: un DAG "produce" un asset, otro lo "consume".
Cuando se produce, el consumidor se dispara automáticamente. Es como dependency graph entre DAGs.

Crea 3 DAGs: uno produce raw data, otro consume raw y produce processed, un tercero consume processed
para analytics. Tags: `['challenge', 'asset_definitions']`.
"""

import datetime

from airflow.sdk import DAG, task, Asset

# TODO: Define Assets y los 3 DAGs con producciones/consumos
