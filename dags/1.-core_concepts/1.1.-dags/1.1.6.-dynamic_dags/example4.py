"""
Dynamic DAGs - Generación con Rangos Numéricos

Demuestra cómo generar tareas basadas en rangos numéricos,
útil para procesar datos particionados o en lotes.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='dynamic_numeric_ranges',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'dynamic_dags']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Procesar datos en 10 particiones
    NUM_PARTITIONS = 10
    
    # Fase 1: Extraer cada partición
    extract_tasks = []
    for partition_id in range(NUM_PARTITIONS):
        extract = BashOperator(
            task_id=f'extract_partition_{partition_id}',
            bash_command=f'echo "📥 Extrayendo partición {partition_id}/{NUM_PARTITIONS - 1}"'
        )
        start >> extract
        extract_tasks.append(extract)
    
    # Fase 2: Procesar cada partición
    process_tasks = []
    for partition_id in range(NUM_PARTITIONS):
        process = BashOperator(
            task_id=f'process_partition_{partition_id}',
            bash_command=f'echo "⚙️ Procesando partición {partition_id}"'
        )
        extract_tasks[partition_id] >> process
        process_tasks.append(process)
    
    # Fase 3: Agrupar particiones en lotes para validación
    BATCH_SIZE = 3
    validation_tasks = []
    
    for batch_num in range(0, NUM_PARTITIONS, BATCH_SIZE):
        batch_end = min(batch_num + BATCH_SIZE, NUM_PARTITIONS)
        validate = BashOperator(
            task_id=f'validate_batch_{batch_num}_to_{batch_end - 1}',
            bash_command=f'echo "✅ Validando lote de particiones {batch_num}-{batch_end - 1}"'
        )
        
        # Conectar las particiones del lote a su validación
        for partition_id in range(batch_num, batch_end):
            process_tasks[partition_id] >> validate
        
        validation_tasks.append(validate)
    
    # Merge final de todas las particiones
    merge = BashOperator(
        task_id='merge_all_partitions',
        bash_command=f'echo "🔗 Consolidando todas las {NUM_PARTITIONS} particiones"'
    )
    
    for validate_task in validation_tasks:
        validate_task >> merge
    
    end = EmptyOperator(task_id='end')
    merge >> end
