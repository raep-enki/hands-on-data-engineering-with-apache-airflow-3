"""
Task Groups - Grupos con Dependencias Cruzadas

Demuestra cómo crear dependencias entre tareas de diferentes grupos,
útil para pipelines donde grupos necesitan coordinarse.
"""

import datetime

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='task_groups_cross_dependencies',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dag_visualization', 'task_groups']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Grupo 1: Procesamiento de datos de ventas
    with TaskGroup(group_id='sales_processing') as sales:
        extract_sales = BashOperator(
            task_id='extract',
            bash_command='echo "📥 Extrayendo datos de ventas"'
        )
        
        validate_sales = BashOperator(
            task_id='validate',
            bash_command='echo "✅ Validando ventas"'
        )
        
        aggregate_sales = BashOperator(
            task_id='aggregate',
            bash_command='echo "📊 Agregando ventas"'
        )
        
        extract_sales >> validate_sales >> aggregate_sales
    
    # Grupo 2: Procesamiento de datos de inventario
    with TaskGroup(group_id='inventory_processing') as inventory:
        extract_inventory = BashOperator(
            task_id='extract',
            bash_command='echo "📥 Extrayendo datos de inventario"'
        )
        
        validate_inventory = BashOperator(
            task_id='validate',
            bash_command='echo "✅ Validando inventario"'
        )
        
        update_inventory = BashOperator(
            task_id='update',
            bash_command='echo "🔄 Actualizando inventario"'
        )
        
        extract_inventory >> validate_inventory >> update_inventory
    
    # Grupo 3: Análisis que depende de ambos grupos anteriores
    with TaskGroup(group_id='cross_analysis') as analysis:
        # Esta tarea necesita datos de ambos grupos
        correlate = BashOperator(
            task_id='correlate_data',
            bash_command='echo "🔗 Correlacionando ventas con inventario"'
        )
        
        predict = BashOperator(
            task_id='predict_demand',
            bash_command='echo "🔮 Prediciendo demanda"'
        )
        
        recommend = BashOperator(
            task_id='recommend_actions',
            bash_command='echo "💡 Recomendando acciones"'
        )
        
        correlate >> predict >> recommend
    
    # Grupo 4: Reportes finales
    with TaskGroup(group_id='reporting') as reporting:
        report_sales = BashOperator(
            task_id='sales_report',
            bash_command='echo "📄 Generando reporte de ventas"'
        )
        
        report_inventory = BashOperator(
            task_id='inventory_report',
            bash_command='echo "📄 Generando reporte de inventario"'
        )
        
        report_analysis = BashOperator(
            task_id='analysis_report',
            bash_command='echo "📄 Generando reporte de análisis"'
        )
        
        # Reportes en paralelo
        [report_sales, report_inventory, report_analysis]
    
    end = EmptyOperator(task_id='end')
    
    # Dependencias principales entre grupos
    start >> [sales, inventory]
    
    # Análisis necesita ambos grupos completados
    [sales, inventory] >> analysis
    
    # Reportes específicos con dependencias cruzadas
    sales >> dag.get_task('reporting.sales_report')
    inventory >> dag.get_task('reporting.inventory_report')
    analysis >> dag.get_task('reporting.analysis_report')
    
    reporting >> end
