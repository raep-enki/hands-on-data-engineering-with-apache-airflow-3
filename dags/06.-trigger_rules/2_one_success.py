"""
Trigger Rules - ONE_SUCCESS Pattern

Demuestra cómo usar ONE_SUCCESS para ejecutar una tarea tan pronto como
al menos una tarea upstream tenga éxito, sin esperar a que todas terminen.
"""

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='trigger_rules_one_success',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'trigger_rules']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    # Múltiples tareas que procesan en paralelo
    # Algunas pueden tardar más que otras
    procesar_rapido = BashOperator(
        task_id='procesar_rapido',
        bash_command='echo "Proceso rápido (5 segundos)" && sleep 5'
    )
    
    procesar_medio = BashOperator(
        task_id='procesar_medio',
        bash_command='echo "Proceso medio (10 segundos)" && sleep 10'
    )
    
    procesar_lento = BashOperator(
        task_id='procesar_lento',
        bash_command='echo "Proceso lento (15 segundos)" && sleep 15'
    )
    
    # Esta tarea se ejecuta tan pronto como UNA tarea upstream tenga éxito
    # No espera a que todas terminen
    notificar_primeros_resultados = BashOperator(
        task_id='notificar_primeros_resultados',
        bash_command='echo "Ya tenemos resultados iniciales disponibles!"',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
    
    # Esta tarea espera a que TODAS las tareas upstream terminen (default)
    consolidar_resultados = BashOperator(
        task_id='consolidar_resultados',
        bash_command='echo "Consolidando todos los resultados"',
        trigger_rule=TriggerRule.ALL_SUCCESS  # Este es el default
    )
    
    fin = EmptyOperator(task_id='fin')
    
    inicio >> [procesar_rapido, procesar_medio, procesar_lento]
    [procesar_rapido, procesar_medio, procesar_lento] >> notificar_primeros_resultados
    [procesar_rapido, procesar_medio, procesar_lento] >> consolidar_resultados
    [notificar_primeros_resultados, consolidar_resultados] >> fin
