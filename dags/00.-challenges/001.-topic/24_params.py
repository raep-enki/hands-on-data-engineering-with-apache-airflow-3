"""
Challenge: Triggerear el DAG Con Parámetros Custom

Hoy quieres procesar la última semana de datos. Mañana quieres procesar del 1 al 15 de noviembre.
Pasado quieres extraer de Salesforce, pero la próxima vez de HubSpot. A veces quieres incluir
registros borrados, a veces no. A veces batch size de 1000, a veces de 5000.

Son decisiones que tomas cuando ejecutas el DAG, no cuando lo escribes. Params te permite definir
"este DAG acepta estos parámetros" con validaciones (start_date debe ser fecha, batch_size entre
100 y 10000, source_system solo estos valores).

Cuando triggeas el DAG desde la UI o API, pasas los valores. El DAG los valida y los usa.

Crea el DAG `params_challenge` diario, sin catchup, con Params validados a nivel DAG y task,
usando @task para procesar basándose en los params. Tags: `['challenge', 'params']`.
"""

import datetime
from airflow.models.param import Param

from airflow.sdk import DAG, task

# TODO: Define Params con validaciones y úsalos en @task
