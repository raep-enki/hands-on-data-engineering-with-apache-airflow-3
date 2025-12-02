"""
Challenge: El Sistema de Monitoreo se Cae Cada Noche

Tu equipo acaba de lanzar un sitio web importante y el CTO quiere un sistema que vigile su salud 24/7.
Te han pedido crear un pipeline que cada 30 minutos revise si todo funciona bien: que el servidor responda,
que los certificados SSL no estén vencidos, que los tiempos de respuesta sean aceptables.

Cuando detecte problemas, debe generar alertas. Cuando todo esté bien, debe actualizar un dashboard 
para que el equipo vea que todo marcha. Tu pipeline será la primera línea de defensa contra caídas.

**El flujo debe ser:**

Primero marcas el inicio del monitoreo (`start_monitoring`). Luego haces un ping básico al sitio 
(`ping_website`). Una vez que sabes que responde, ejecutas en paralelo tres verificaciones: revisar 
el código HTTP (`check_http_status`), medir cuánto tarda en responder (`check_response_time`), y 
validar el certificado SSL (`check_ssl_certificate`).

Después de las verificaciones, pones un checkpoint (`health_checkpoint`) para marcar que todo pasó.
Luego calculas en paralelo estadísticas: uptime del último período (`analyze_uptime`) y métricas 
de performance (`analyze_performance`). Con esos datos, generas alertas si algo anda mal 
(`generate_alerts`). Finalmente, en paralelo actualizas el dashboard visual (`update_dashboard`) 
y envías un reporte por email (`send_report`). Terminas marcando el fin del ciclo (`end_monitoring`).

**Configuración técnica:**
- DAG ID: `website_monitoring_pipeline`
- Schedule: `*/30 * * * *` (cada media hora)
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'declaring_a_dag']`
- Usa el estilo context manager (`with DAG(...)`)
- Total: 12 tareas (puedes simular los comandos con echo)
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Crea el pipeline de monitoreo
