"""
Challenge: Pasar Objetos Complejos Entre Tareas

En el challenge anterior pasabas números simples o strings con XComs. Pero ¿qué pasa cuando necesitas
pasar estructuras complejas? Configuraciones de ML con 20 hiperparámetros, estadísticas de features,
artefactos de modelos con metadata, reportes con matrices y métricas complejas.

Python tiene **dataclasses** perfectas para esto: defines la estructura de tus datos como clases
tipadas, y TaskFlow las serializa/deserializa automáticamente. Es como pasar objetos entre funciones,
pero las funciones están en tareas distribuidas.

**IMPORTANTE: Todas las tareas en este challenge usan PythonOperator (@task)** porque involucran:
- Procesamiento de objetos Python complejos
- Validaciones y transformaciones de datos
- Lógica de machine learning
- Decisiones basadas en métricas

No uses BashOperator para lógica compleja. Reserva bash para comandos shell simples.

**El pipeline de ML completo (config → data → train → evaluate → deploy):**

Crea el DAG `passing_objects_challenge` con:
- 4 dataclasses: `ModelConfig`, `DatasetStats`, `TrainedModel`, `EvaluationMetrics`
- 9 tareas @task que pasen estos objetos entre sí
- Branching basado en métricas de evaluación
- Reporte consolidando TODOS los objetos

**Configuración técnica:**
- DAG ID: `passing_objects_challenge`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'passing_arbitrary_objects_as_arguments']`
- Usa PythonOperator (@task) para TODAS las tareas (no BashOperator)
"""

import datetime
from dataclasses import dataclass

from airflow.sdk import DAG, task

# TODO: Define dataclasses y funciones @task que las pasen
