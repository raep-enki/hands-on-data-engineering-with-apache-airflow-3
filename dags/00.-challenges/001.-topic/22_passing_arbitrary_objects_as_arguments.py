"""
Challenge: Pasar Objetos Complejos Entre Tareas

En el challenge anterior pasabas números simples o strings. Pero ¿qué pasa cuando necesitas pasar
estructuras complejas? Configuraciones de modelos con hiperparámetros, DataFrames con miles de filas,
objetos con métodos, resultados de evaluación con múltiples métricas.

Python tiene "dataclasses" perfectas para esto: defines la estructura de tus datos (ModelConfig,
FeatureStats, ModelArtifact, EvaluationReport), y TaskFlow automáticamente los serializa/deserializa
entre tareas. Es como pasar objetos entre funciones, pero las funciones están en tareas diferentes.

Tu pipeline de ML necesita: cargar config → fetch data → preprocess → train → evaluate → deploy.
Cada paso pasa objetos ricos al siguiente: no solo "train model", sino "ModelConfig con 20 parámetros".

Crea el DAG `passing_objects_challenge` diario, sin catchup, usando @task con dataclasses
para pasar objetos complejos. Tags: `['challenge', 'passing_arbitrary_objects_as_arguments']`.
"""

import datetime
from dataclasses import dataclass

from airflow.sdk import DAG, task

# TODO: Define dataclasses y funciones @task que las pasen
