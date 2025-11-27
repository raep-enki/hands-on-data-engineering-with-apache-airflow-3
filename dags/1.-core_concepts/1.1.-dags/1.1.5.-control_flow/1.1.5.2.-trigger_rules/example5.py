"""
Trigger Rules - ALWAYS Pattern para Monitoreo Continuo

Demuestra cómo usar ALWAYS para tareas que deben ejecutarse sin importar
el estado de las tareas upstream, útil para monitoreo y métricas.
"""

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='trigger_rules_always',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'trigger_rules']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    # Tareas principales que pueden tener éxito o fallar
    extraer_datos = BashOperator(
        task_id='extraer_datos',
        bash_command='echo "Extrayendo datos de múltiples fuentes"'
    )
    
    validar_datos = BashOperator(
        task_id='validar_datos',
        bash_command='echo "Validando calidad de datos"'
    )
    
    transformar_datos = BashOperator(
        task_id='transformar_datos',
        bash_command='echo "Aplicando transformaciones"'
    )
    
    cargar_datos = BashOperator(
        task_id='cargar_datos',
        bash_command='echo "Cargando datos al warehouse"'
    )
    
    # Tareas de monitoreo que SIEMPRE se ejecutan
    # No importa si las tareas anteriores tuvieron éxito o fallaron
    
    registrar_metricas = BashOperator(
        task_id='registrar_metricas',
        bash_command='echo "📊 Registrando métricas de ejecución (duración, recursos, etc)"',
        trigger_rule=TriggerRule.ALWAYS
    )
    
    actualizar_dashboard = BashOperator(
        task_id='actualizar_dashboard',
        bash_command='echo "📈 Actualizando dashboard de monitoreo"',
        trigger_rule=TriggerRule.ALWAYS
    )
    
    enviar_heartbeat = BashOperator(
        task_id='enviar_heartbeat',
        bash_command='echo "💓 Enviando heartbeat al sistema de monitoreo"',
        trigger_rule=TriggerRule.ALWAYS
    )
    
    # Tarea de auditoría que registra todo lo que pasó
    auditoria_completa = BashOperator(
        task_id='auditoria_completa',
        bash_command='echo "📝 Registrando auditoría completa de la ejecución"',
        trigger_rule=TriggerRule.ALWAYS
    )
    
    # Tarea de notificación según resultado
    # Esta sí depende del estado (usa ALL_DONE)
    notificar_resultado = BashOperator(
        task_id='notificar_resultado',
        bash_command='echo "📧 Enviando notificación del resultado final"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    fin = EmptyOperator(
        task_id='fin',
        trigger_rule=TriggerRule.ALWAYS
    )
    
    # Flujo principal
    inicio >> extraer_datos >> validar_datos >> transformar_datos >> cargar_datos
    
    # Tareas de monitoreo se ejecutan en paralelo sin importar el estado
    cargar_datos >> [registrar_metricas, actualizar_dashboard, enviar_heartbeat]
    
    # Auditoría completa después del monitoreo
    [registrar_metricas, actualizar_dashboard, enviar_heartbeat] >> auditoria_completa
    
    # Notificación y fin
    auditoria_completa >> notificar_resultado >> fin
