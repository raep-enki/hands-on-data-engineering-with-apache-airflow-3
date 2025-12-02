"""
Edge Labels - Labels para Condiciones de Error

Demuestra cómo usar labels para documentar manejo de errores
y flujos alternativos cuando algo falla.
"""

import datetime

from airflow.sdk import DAG, Label, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='edge_labels_error_handling',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dag_visualization', 'edge_labels']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Proceso principal que puede fallar
    extract = BashOperator(
        task_id='extract_from_api',
        bash_command='echo "📥 Extrayendo datos de API"'
    )
    
    validate = BashOperator(
        task_id='validate_response',
        bash_command='echo "✅ Validando respuesta"'
    )
    
    # Camino de éxito
    transform = BashOperator(
        task_id='transform_success',
        bash_command='echo "⚙️ Transformando datos válidos"'
    )
    
    load = BashOperator(
        task_id='load_to_warehouse',
        bash_command='echo "📤 Cargando a warehouse"'
    )
    
    # Camino de error/recuperación
    log_error = BashOperator(
        task_id='log_error',
        bash_command='echo "❌ Registrando error"',
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    notify_team = BashOperator(
        task_id='notify_team',
        bash_command='echo "📧 Notificando al equipo"'
    )
    
    use_fallback = BashOperator(
        task_id='use_fallback_data',
        bash_command='echo "🔄 Usando datos de respaldo"'
    )
    
    # Punto de convergencia
    report = BashOperator(
        task_id='generate_report',
        bash_command='echo "📊 Generando reporte"',
        trigger_rule=TriggerRule.ALL_DONE
    )
    
    end = EmptyOperator(task_id='end')
    
    # Flujo con labels documentando las condiciones
    start >> Label('Iniciar extracción') >> extract
    extract >> Label('Datos extraídos') >> validate
    
    # Camino de éxito
    validate >> Label('✅ Validación OK') >> transform
    transform >> Label('Datos listos') >> load
    load >> Label('Carga completa') >> report
    
    # Camino de error
    validate >> Label('❌ Error detectado') >> log_error
    log_error >> Label('Error registrado') >> notify_team
    notify_team >> Label('Equipo notificado') >> use_fallback
    use_fallback >> Label('Usando fallback') >> report
    
    report >> Label('Finalizado') >> end
