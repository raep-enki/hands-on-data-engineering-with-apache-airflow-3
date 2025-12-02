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

# Solución del challenge
with DAG(
    dag_id='website_monitoring_pipeline',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='*/30 * * * *',
    catchup=False,
    tags=['challenge', 'declaring_a_dag'],
) as dag:
    
    start_monitoring = EmptyOperator(task_id='start_monitoring')
    
    ping_website = BashOperator(
        task_id='ping_website',
        bash_command='echo "Ping: website is responding"',
    )
    
    check_http_status = BashOperator(
        task_id='check_http_status',
        bash_command='echo "HTTP Status: 200 OK"',
    )
    
    check_response_time = BashOperator(
        task_id='check_response_time',
        bash_command='echo "Response time: 125ms"',
    )
    
    check_ssl_certificate = BashOperator(
        task_id='check_ssl_certificate',
        bash_command='echo "SSL: Valid until 2025-12-31"',
    )
    
    health_checkpoint = EmptyOperator(task_id='health_checkpoint')
    
    analyze_uptime = BashOperator(
        task_id='analyze_uptime',
        bash_command='echo "Uptime: 99.95%"',
    )
    
    analyze_performance = BashOperator(
        task_id='analyze_performance',
        bash_command='echo "Avg response: 110ms"',
    )
    
    generate_alerts = BashOperator(
        task_id='generate_alerts',
        bash_command='echo "No alerts generated"',
    )
    
    update_dashboard = BashOperator(
        task_id='update_dashboard',
        bash_command='echo "Dashboard updated successfully"',
    )
    
    send_report = BashOperator(
        task_id='send_report',
        bash_command='echo "Report sent to team@company.com"',
    )
    
    end_monitoring = EmptyOperator(task_id='end_monitoring')
    
    # Dependencies
    start_monitoring >> ping_website
    ping_website >> [check_http_status, check_response_time, check_ssl_certificate]
    [check_http_status, check_response_time, check_ssl_certificate] >> health_checkpoint
    health_checkpoint >> [analyze_uptime, analyze_performance]
    [analyze_uptime, analyze_performance] >> generate_alerts
    generate_alerts >> [update_dashboard, send_report]
    [update_dashboard, send_report] >> end_monitoring
