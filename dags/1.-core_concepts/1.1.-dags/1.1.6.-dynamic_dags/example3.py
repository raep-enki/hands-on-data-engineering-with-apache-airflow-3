"""
Dynamic DAGs - Generación desde Configuración

Demuestra cómo generar tareas dinámicamente desde una estructura
de configuración (diccionario), útil para configuraciones complejas.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Configuración de fuentes de datos
DATA_SOURCES = {
    'customers': {
        'table': 'customers',
        'priority': 'high',
        'retention_days': 90
    },
    'orders': {
        'table': 'orders',
        'priority': 'high',
        'retention_days': 365
    },
    'products': {
        'table': 'products',
        'priority': 'medium',
        'retention_days': 180
    },
    'analytics': {
        'table': 'page_views',
        'priority': 'low',
        'retention_days': 30
    }
}

with DAG(
    dag_id='dynamic_from_config',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dynamic_dags']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Tareas para fuentes de alta prioridad
    high_priority_tasks = []
    
    # Tareas para fuentes de media/baja prioridad
    other_priority_tasks = []
    
    # Generar tareas basadas en la configuración
    for source_name, config in DATA_SOURCES.items():
        sync_task = BashOperator(
            task_id=f'sync_{source_name}',
            bash_command=f'echo "🔄 Sync {config["table"]} (priority: {config["priority"]}, retention: {config["retention_days"]} days)"'
        )
        
        # Organizar por prioridad
        if config['priority'] == 'high':
            high_priority_tasks.append(sync_task)
        else:
            other_priority_tasks.append(sync_task)
        
        start >> sync_task
    
    # Checkpoint después de alta prioridad
    checkpoint = EmptyOperator(task_id='high_priority_complete')
    
    # Alta prioridad debe completarse primero
    for task in high_priority_tasks:
        task >> checkpoint
    
    # Otras prioridades pueden empezar después del checkpoint
    for task in other_priority_tasks:
        checkpoint >> task
    
    # Validación final
    validate = BashOperator(
        task_id='validate_all_syncs',
        bash_command='echo "✅ Validando todas las sincronizaciones"'
    )
    
    # Todas las tareas deben terminar antes de validar
    for task in high_priority_tasks + other_priority_tasks:
        task >> validate
    
    end = EmptyOperator(task_id='end')
    validate >> end
