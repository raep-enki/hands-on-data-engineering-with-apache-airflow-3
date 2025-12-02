"""
Trigger Rules - NONE_FAILED Pattern

Demuestra cómo usar NONE_FAILED para ejecutar una tarea si ninguna
tarea upstream falló, incluso si algunas fueron skipped.
"""

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='trigger_rules_none_failed',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'trigger_rules']
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    def choose_validation_level(**context):
        """Decide qué nivel de validación ejecutar"""
        day = context['logical_date'].day
        if day <= 10:
            return 'validacion_completa'
        return 'validacion_basica'
    
    branch_validation = BranchPythonOperator(
        task_id='decidir_validacion',
        python_callable=choose_validation_level,
    )
    
    # Dos niveles de validación
    validacion_completa = BashOperator(
        task_id='validacion_completa',
        bash_command='echo "Ejecutando validación exhaustiva"'
    )
    
    validacion_basica = BashOperator(
        task_id='validacion_basica',
        bash_command='echo "Ejecutando validación básica"'
    )
    
    # Esta tarea se ejecuta si NINGUNA tarea upstream falló
    # Se ejecutará incluso si una de las validaciones fue skipped
    procesar_datos = BashOperator(
        task_id='procesar_datos',
        bash_command='echo "Procesando datos - validación pasó"',
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    # Esta tarea requiere que TODAS las tareas upstream tuvieran éxito
    # NO se ejecutará si una fue skipped (default behavior)
    generar_reporte_completo = BashOperator(
        task_id='generar_reporte_completo',
        bash_command='echo "Generando reporte completo"',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    fin = EmptyOperator(
        task_id='fin',
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    inicio >> branch_validation >> [validacion_completa, validacion_basica]
    [validacion_completa, validacion_basica] >> procesar_datos
    [validacion_completa, validacion_basica] >> generar_reporte_completo
    [procesar_datos, generar_reporte_completo] >> fin
