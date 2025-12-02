"""
Challenge: Configuración Que No Vive en el Código

Tu pipeline necesita conectarse a bases de datos. Los hosts y puertos cambian entre desarrollo,
staging y producción. Las API keys son secretas. Los batch sizes varían. Las configuraciones de
email también. NO quieres hardcodear esto en tu código.

La solución: Airflow Variables. Las guardas en la UI de Airflow (o las importas de archivos),
y tu código las lee. Cambias un valor en la UI, y todos los DAGs usan el nuevo valor sin cambiar código.

Tu pipeline debe leer el environment (dev/staging/prod) y según eso, usar diferentes hosts, puertos,
API keys. Debe respetar batch sizes configurables, saber si enviar notificaciones o no, cuántos
retries intentar.

Crea el DAG `variables_challenge` diario, sin catchup, usando @task que lean Variables de Airflow
para configuración multi-environment. Tags: `['challenge', 'variables']`.
"""

import datetime

from airflow.sdk import DAG, task, Variable

# TODO: Implementa funciones @task que usen Variables de Airflow
