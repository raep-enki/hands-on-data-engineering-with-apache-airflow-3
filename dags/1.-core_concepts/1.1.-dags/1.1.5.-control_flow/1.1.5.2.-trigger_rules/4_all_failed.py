"""
Trigger Rules - ALL_FAILED y ONE_FAILED Patterns

Demuestra cómo usar trigger rules para manejar escenarios de fallo
y ejecutar tareas de recuperación o alertas.
"""

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='trigger_rules_failure_handling',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'trigger_rules']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    # Tareas que pueden fallar
    # Para simular fallo, cambiar bash_command a "exit 1"
    extraer_api_a = BashOperator(
        task_id='extraer_api_a',
        bash_command='echo "Extrayendo de API A" && sleep 2'
    )
    
    extraer_api_b = BashOperator(
        task_id='extraer_api_b',
        bash_command='echo "Extrayendo de API B" && sleep 2'
    )
    
    extraer_api_c = BashOperator(
        task_id='extraer_api_c',
        bash_command='echo "Extrayendo de API C" && sleep 2'
    )
    
    # Se ejecuta tan pronto como UNA tarea falle
    # Útil para alertas tempranas
    alerta_temprana = BashOperator(
        task_id='alerta_temprana',
        bash_command='echo "⚠️ ALERTA: Al menos una extracción falló"',
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    # Se ejecuta solo si TODAS las tareas fallaron
    # Útil para recuperación completa
    recuperacion_completa = BashOperator(
        task_id='recuperacion_completa',
        bash_command='echo "🔴 CRÍTICO: Todas las extracciones fallaron - iniciando recuperación"',
        trigger_rule=TriggerRule.ALL_FAILED
    )
    
    # Se ejecuta solo si al menos una tarea tuvo éxito
    procesar_datos_disponibles = BashOperator(
        task_id='procesar_datos_disponibles',
        bash_command='echo "✅ Procesando datos disponibles"',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
    
    # Se ejecuta siempre, sin importar el resultado
    limpiar_recursos = BashOperator(
        task_id='limpiar_recursos',
        bash_command='echo "🧹 Limpiando recursos temporales"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    fin = EmptyOperator(
        task_id='fin',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    inicio >> [extraer_api_a, extraer_api_b, extraer_api_c]
    [extraer_api_a, extraer_api_b, extraer_api_c] >> alerta_temprana
    [extraer_api_a, extraer_api_b, extraer_api_c] >> recuperacion_completa
    [extraer_api_a, extraer_api_b, extraer_api_c] >> procesar_datos_disponibles
    [alerta_temprana, recuperacion_completa, procesar_datos_disponibles] >> limpiar_recursos >> fin
