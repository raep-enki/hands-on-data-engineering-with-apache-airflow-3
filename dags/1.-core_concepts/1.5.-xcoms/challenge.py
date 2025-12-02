"""
Challenge: Sistema de Procesamiento Multi-Stage con XComs

Objetivo:
=========
Crear UN ÚNICO DAG que procese datos en múltiples stages,
usando XComs para compartir datos y metadata entre tareas.

Requisitos:
===========

1. DAG: 'xcoms_challenge'
   - Schedule: @daily
   - Catchup: False
   - Tags: ['challenge', 'core_concepts', 'xcoms']

2. Pipeline (8 tareas Python con PythonOperator):

   A. extract_from_sources:
      - Simula extracción de 3 fuentes
      - Return dict principal: {'records': list, 'count': int}
      - XCom push key='source_metadata': {'sources': ['api', 'db', 's3'], 'timestamp': str}
      - XCom push key='extraction_stats': {'duration_sec': float, 'errors': int}
   
   B. validate_quality:
      - Pull main data de extract_from_sources
      - Valida que count > 0
      - Calcula quality_score (0-100)
      - Return: {'valid': bool, 'quality_score': int, 'data': original_data}
      - XCom push key='validation_report': {'passed': bool, 'issues': list}
   
   C. enrich_data:
      - Pull validated data
      - Solo enriquece si quality_score >= 70
      - Agrega campos: 'enriched': True, 'enriched_at': timestamp
      - Return data enriquecida
      - XCom push key='enrichment_stats': {'fields_added': int, 'time_sec': float}
   
   D. split_for_processing:
      - Pull enriched data
      - Divide records en 3 batches
      - Return: {'batch_1': list, 'batch_2': list, 'batch_3': list}
      - XCom push key='split_metadata': {'batch_count': 3, 'sizes': [int, int, int]}
   
   E. process_batch_1:
      - Pull batches de split_for_processing
      - Procesa solo batch_1
      - Return: {'batch_id': 1, 'processed_count': int, 'sum': int}
   
   F. process_batch_2:
      - Pull batches, procesa batch_2
      - Return: {'batch_id': 2, 'processed_count': int, 'sum': int}
   
   G. process_batch_3:
      - Pull batches, procesa batch_3
      - Return: {'batch_id': 3, 'processed_count': int, 'sum': int}
   
   H. aggregate_and_report:
      - Pull results de process_batch_1, 2, 3 (usando lista de task_ids)
      - Pull metadata de extract (source_metadata, extraction_stats)
      - Pull metadata de validate (validation_report)
      - Pull metadata de enrich (enrichment_stats)
      - Pull metadata de split (split_metadata)
      - Genera reporte completo con:
        * Total records processed
        * Total sum de todos los batches
        * Pipeline metadata (sources, quality, enrichment)
      - Return: dict con reporte completo
      - XCom push key='final_report': dict con todo

3. Dependencias:
   extract_from_sources >> validate_quality >> enrich_data >> 
   split_for_processing >> [process_batch_1, process_batch_2, process_batch_3] >>
   aggregate_and_report

4. Datos de ejemplo:
   - extract: generar 30 records [{'id': i, 'value': i*10} for i in range(1, 31)]
   - quality_score: random entre 80-95
   - batches: dividir 30 records en 3 batches de 10 cada uno

5. Usar PythonOperator (no @task) para práctica explícita de XCom

Restricciones:
==============
- NO usar @task (usar PythonOperator)
- Todas las funciones deben recibir **context
- Usar ti = context['ti'] para XCom
- Push múltiples keys por tarea (main return + keys adicionales)
- Pull selectivo según necesidad
- Prints con emojis descriptivos

Tips:
=====
- ti.xcom_push(key='name', value={...})
- ti.xcom_pull(task_ids='task_name') → return value
- ti.xcom_pull(task_ids='task_name', key='custom_key') → specific key
- ti.xcom_pull(task_ids=['task1', 'task2', 'task3']) → list of values
- Para batches: batch = data['batch_1'], batch_sum = sum(r['value'] for r in batch)

Evaluación:
===========
- ✅ XCom push de múltiples keys por tarea
- ✅ Pull de task individual y múltiples tasks
- ✅ Pull de returns y keys custom
- ✅ Metadata separada de datos principales
- ✅ Agregación correcta de múltiples sources
- ✅ Reporte final con toda la metadata del pipeline
"""

# TU CÓDIGO AQUÍ
