"""
Challenge: ETL Multi-Source con Dynamic Task Mapping

Objetivo:
=========
Crear UN ÚNICO DAG que procese múltiples fuentes de datos
usando dynamic task mapping para paralelizar el procesamiento.

Requisitos:
===========

1. DAG: 'dynamic_mapping_challenge'
   - Schedule: @daily
   - Catchup: False
   - Tags: ['challenge', 'authoring_and_scheduling', 'dynamic_task_mapping', 'simple_mapping']

2. Pipeline (6 tareas):

   A. generate_sources (@task):
      - Genera lista de 5 sources:
        ```python
        [
          {'name': 'api_users', 'type': 'api', 'batch_size': 1000},
          {'name': 'db_orders', 'type': 'database', 'batch_size': 1500},
          {'name': 's3_logs', 'type': 's3', 'batch_size': 2000},
          {'name': 'api_products', 'type': 'api', 'batch_size': 1200},
          {'name': 'db_customers', 'type': 'database', 'batch_size': 1800}
        ]
        ```
      - Return: lista de dicts
   
   B. extract_from_source (@task con expand_kwargs):
      - Recibe: name, type, batch_size (de cada source)
      - Simula extracción:
        * Print "Extracting from {name} ({type})"
        * records = batch_size * 5 (simulado)
      - Return: {'name': name, 'type': type, 'records': records}
      - Usa expand_kwargs sobre generate_sources
   
   C. validate_extraction (@task con expand):
      - Recibe: extraction_result (de cada extract_from_source)
      - Valida que records > 0
      - Si inválido: agregar a errors list
      - Return: {'source': name, 'records': records, 'valid': bool}
      - Usa expand sobre extract_from_source results
   
   D. transform_by_type (@task_group con expand):
      - Recibe: validated_data
      - Task group interno con 2 tasks:
        * apply_transformation: transforma según type
          - Si type == 'api': records * 1.1
          - Si type == 'database': records * 1.2
          - Si type == 's3': records * 1.0
        * add_metadata: agrega timestamp y processed=True
      - Return: transformed_data
      - Usa expand sobre validate_extraction results
   
   E. aggregate_by_type (@task):
      - Recibe: todos los transformed results (lista)
      - Agrega por type:
        * api_total: suma de records tipo api
        * database_total: suma de records tipo database
        * s3_total: suma de records tipo s3
      - Return: {'by_type': dict, 'grand_total': int}
   
   F. generate_report (@task):
      - Recibe: aggregated_data
      - Genera reporte con:
        * Sources procesados
        * Total records por tipo
        * Grand total
        * Success rate (valid / total)
      - Return: report dict

3. Dependencias:
   generate_sources >> extract (expand_kwargs) >> validate (expand) >>
   transform (task_group expand) >> aggregate >> report

4. Restricciones:
   - extract_from_source usa expand_kwargs(sources)
   - validate_extraction usa expand(extraction_result=extractions)
   - transform_by_type es task_group con expand(validated_data=validated)
   - No usar loops manuales - todo con expand()
   - Usar @task y @task_group

Tips:
=====
- @task def func(...): ... luego func.expand_kwargs(list_of_dicts)
- @task def func(arg): ... luego func.expand(arg=list_of_values)
- @task_group def group(arg): ... luego group.expand(arg=list_of_values)
- En aggregate: results es lista, iterar: for r in results
- Type aggregation: use dict groupby pattern

Evaluación:
===========
- ✅ expand_kwargs usado correctamente
- ✅ expand usado para single argument
- ✅ task_group con expand funcionando
- ✅ Paralelización automática
- ✅ Aggregation de múltiples mapped results
- ✅ Reporte final correcto

Output esperado en logs:
=========================
```
Extracting from api_users (api)
Extracting from db_orders (database)
...
Aggregating 5 sources by type
  - api: 11000 records
  - database: 19800 records
  - s3: 10000 records
Grand total: 40800 records
```
"""

# TU CÓDIGO AQUÍ
