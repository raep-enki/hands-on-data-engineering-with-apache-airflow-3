"""
Dynamic DAGs - Generación de Múltiples DAGs

Demuestra cómo generar múltiples DAGs completos dinámicamente
desde una configuración, útil para multi-tenancy o entornos similares.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Configuración de clientes/tenants
TENANTS = [
    {'name': 'acme_corp', 'schedule': '@hourly', 'priority': 'high'},
    {'name': 'globex', 'schedule': '@daily', 'priority': 'medium'},
    {'name': 'initech', 'schedule': '@daily', 'priority': 'low'},
    {'name': 'umbrella', 'schedule': '0 */6 * * *', 'priority': 'high'},
]

def create_tenant_dag(tenant_name, schedule, priority):
    """
    Factory function para crear un DAG por tenant
    """
    dag = DAG(
        dag_id=f'tenant_{tenant_name}_pipeline',
        schedule=schedule,
        start_date=datetime.datetime(2021, 1, 1),
        catchup=False,
        tags=['example', 'core_concepts', 'dags', 'dynamic_dags', tenant_name, priority],
        default_args={'owner': tenant_name}
    )
    
    with dag:
        start = EmptyOperator(task_id='start')
        
        # Extracción específica del tenant
        extract = BashOperator(
            task_id='extract_data',
            bash_command=f'echo "📥 Extrayendo datos para {tenant_name} (prioridad: {priority})"'
        )
        
        # Validación de datos
        validate = BashOperator(
            task_id='validate_data',
            bash_command=f'echo "✅ Validando datos de {tenant_name}"'
        )
        
        # Procesamiento
        process = BashOperator(
            task_id='process_data',
            bash_command=f'echo "⚙️ Procesando datos de {tenant_name}"'
        )
        
        # Carga en data warehouse del tenant
        load = BashOperator(
            task_id='load_to_warehouse',
            bash_command=f'echo "📤 Cargando datos al warehouse de {tenant_name}"'
        )
        
        # Notificación
        notify = BashOperator(
            task_id='notify_completion',
            bash_command=f'echo "📧 Notificando a {tenant_name} - proceso completado"'
        )
        
        end = EmptyOperator(task_id='end')
        
        # Pipeline
        start >> extract >> validate >> process >> load >> notify >> end
    
    return dag

# Generar un DAG por cada tenant
for tenant in TENANTS:
    dag_id = f'tenant_{tenant["name"]}_pipeline'
    globals()[dag_id] = create_tenant_dag(
        tenant_name=tenant['name'],
        schedule=tenant['schedule'],
        priority=tenant['priority']
    )
