"""
Task Groups - Grupos Dinámicos

Demuestra cómo generar task groups dinámicamente,
combinando la generación dinámica con organización visual.
"""

import datetime

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_groups_dynamic',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dag_visualization', 'task_groups']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Lista de servicios a monitorear
    services = ['web_app', 'api_service', 'database', 'cache']
    
    # Almacenar referencias a task groups
    service_groups = {}
    
    # Crear un grupo por cada servicio
    for service in services:
        with TaskGroup(group_id=f'{service}_monitoring') as service_group:
            
            check_health = BashOperator(
                task_id='check_health',
                bash_command=f'echo "🏥 Verificando salud de {service}"'
            )
            
            check_performance = BashOperator(
                task_id='check_performance',
                bash_command=f'echo "⚡ Verificando performance de {service}"'
            )
            
            check_errors = BashOperator(
                task_id='check_errors',
                bash_command=f'echo "🔍 Verificando errores de {service}"'
            )
            
            # Checks en paralelo dentro del grupo
            [check_health, check_performance, check_errors]
        
        # Almacenar referencia y conectar
        service_groups[service] = service_group
        start >> service_group
    
    # Consolidar todos los resultados
    consolidate = BashOperator(
        task_id='consolidate_monitoring_results',
        bash_command='echo "📊 Consolidando resultados de monitoreo"'
    )
    
    # Todos los grupos de servicios convergen en consolidate
    for service in services:
        service_groups[service] >> consolidate
    
    end = EmptyOperator(task_id='end')
    consolidate >> end
