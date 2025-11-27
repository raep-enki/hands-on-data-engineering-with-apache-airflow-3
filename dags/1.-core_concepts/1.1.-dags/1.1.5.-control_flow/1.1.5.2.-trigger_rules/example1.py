"""
Trigger Rules - Join Pattern después de Branching

Demuestra cómo usar trigger_rule ALL_DONE para crear un "join point" que se ejecuta
sin importar qué rama se tomó en un branching previo.
"""

import pendulum

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='trigger_rules_join',
    schedule='@once',
    start_date=pendulum.datetime(2019, 2, 28, tz='UTC'),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'control_flow', 'trigger_rules']
) as dag:
    
    run_this_first = EmptyOperator(task_id='run_this_first')
    
    def do_branching(**context):
        """Función que decide qué rama tomar"""
        return 'branch_a'
    
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=do_branching,
    )
    
    branch_a = EmptyOperator(task_id='branch_a')
    follow_branch_a = EmptyOperator(task_id='follow_branch_a')
    
    branch_b = EmptyOperator(task_id='branch_b')
    
    # Esta tarea usa trigger_rule ALL_DONE para ejecutarse después del branch
    # sin importar qué rama se tomó (incluso si algunas tareas fueron skipped)
    join = EmptyOperator(
        task_id='join',
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    run_this_first >> branching
    branching >> branch_a >> follow_branch_a >> join
    branching >> branch_b >> join
