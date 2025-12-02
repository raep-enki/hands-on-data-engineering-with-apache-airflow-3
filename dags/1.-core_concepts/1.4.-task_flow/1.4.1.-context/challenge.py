"""
Challenge: Sistema ETL Configurable con Context

Objetivo:
=========
Crear UN ÚNICO DAG que implemente un ETL completo usando TaskFlow API
con configuración runtime, context variables, y manejo de fechas.

Requisitos:
===========

1. DAG: 'context_challenge'
   - Schedule: @hourly
   - Catchup: False
   - Tags: ['challenge', 'core_concepts', 'task_flow', 'context']

2. Runtime Configuration (dag_run.conf):
   El DAG debe aceptar la siguiente configuración opcional:
   
   - environment: 'development' | 'staging' | 'production' (default: 'production')
   - data_source: 'api' | 'database' | 's3' (default: 'api')
   - batch_size: integer > 0 (default: 1000)
   - enable_validation: boolean (default: True)
   - enable_enrichment: boolean (default: True)
   - target_format: 'json' | 'parquet' | 'csv' (default: 'parquet')

3. Estructura del pipeline (6 tareas @task):

   A. validate_configuration:
      - Accede a **context para obtener dag_run.conf
      - Valida que todos los valores sean correctos:
        * environment en ['development', 'staging', 'production']
        * data_source en ['api', 'database', 's3']
        * batch_size > 0
        * target_format en ['json', 'parquet', 'csv']
      - Si inválido, raise ValueError con mensaje descriptivo
      - Si válido, retorna config completo con defaults aplicados
   
   B. extract_data:
      - Recibe config de validate_configuration
      - Recibe logical_date y data_interval_start/end del context
      - Simula extracción según data_source:
        * api: "Calling API for {date}"
        * database: "Querying DB for interval {start} to {end}"
        * s3: "Reading S3 files for {date}"
      - Usa batch_size para limitar registros
      - Retorna dict con:
        * records: lista simulada de N registros (N = batch_size)
        * source: data_source usado
        * extraction_time: str(datetime.now())
        * data_date: str(logical_date)
      - Usa ti.xcom_push para guardar metadata:
        * key='extract_metadata'
        * value: {source, record_count, extraction_time}
   
   C. conditional_validation:
      - Recibe extracted_data
      - Recibe config
      - Solo valida si enable_validation=True
      - Si False, retorna extracted_data sin cambios
      - Si True:
        * Imprime "Running validation checks..."
        * Valida que hay registros
        * Valida que todos tengan 'id' field
        * Retorna extracted_data con campo validation_passed=True
   
   D. conditional_enrichment:
      - Recibe validated_data
      - Recibe config y logical_date del context
      - Solo enriquece si enable_enrichment=True
      - Si False, retorna validated_data sin cambios
      - Si True:
        * Imprime "Enriching data..."
        * Agrega a cada record:
          - processed_date: str(logical_date)
          - environment: config['environment']
        * Retorna data enriquecida
   
   E. generate_output_path:
      - Recibe logical_date del context
      - Recibe config
      - Genera path particionado:
        * Base: s3://data-lake/{environment}/
        * Partición: year={year}/month={month}/day={day}/hour={hour}/
        * Filename: data_{timestamp}.{target_format}
      - Retorna dict con: {base_path, partition, filename, full_path}
   
   F. load_data:
      - Recibe enriched_data
      - Recibe output_path
      - Recibe ti del context
      - Pull extract_metadata del XCom
      - Imprime resumen completo:
        * Source: {data_source}
        * Records: {count}
        * Output: {full_path}
        * Format: {target_format}
        * Extraction time: {extraction_time}
      - Retorna dict con:
        * status: 'success'
        * records_loaded: count
        * output_path: full_path
        * metadata: extract_metadata from XCom

4. Dependencias:
   validate_configuration >> extract_data >> conditional_validation >> 
   conditional_enrichment >> load_data
   
   generate_output_path se ejecuta en paralelo y converge en load_data:
   validate_configuration >> generate_output_path >> load_data

5. Documentación:
   - DAG doc_md que explique:
     * Configuraciones aceptadas con tipos y defaults
     * Ejemplo de trigger con cada configuración
     * Cómo las tareas usan logical_date
     * Qué metadata se guarda en XCom
     * Lógica condicional (validation/enrichment)

Restricciones:
==============
- Usar SOLO @task decorator (TaskFlow API)
- Acceder a context con parámetros específicos o **context
- Un solo DAG en el archivo
- Simular datos (no conexiones reales)
- Todos los prints deben tener emojis descriptivos

Estructura de datos:
====================
Records simulados deben ser:
[
  {"id": 1, "value": 100, "status": "active"},
  {"id": 2, "value": 200, "status": "active"},
  ...
]

Tips:
=====
- Para simular N records: [{"id": i, "value": i*100, "status": "active"} for i in range(1, batch_size+1)]
- logical_date.hour para la hora en el path particionado
- config.get('key', default) para valores con defaults
- Validación debe raise ValueError si detecta problema
- XCom push manual para metadata, return para datos principales
- Tareas condicionales usan if/else basándose en config

Evaluación:
===========
- ✅ Validación correcta de configuración
- ✅ Uso apropiado de logical_date y data_interval
- ✅ XCom manual para metadata
- ✅ Returns para paso de datos principal
- ✅ Lógica condicional funcionando
- ✅ Particionado temporal correcto
- ✅ Context access variado (**context, params específicos)
- ✅ Documentación completa

Ejemplo de trigger:
===================
# Default (todo production):
airflow dags trigger context_challenge

# Development con validación deshabilitada:
airflow dags trigger context_challenge --conf '{
  "environment": "development",
  "data_source": "database",
  "batch_size": 100,
  "enable_validation": false,
  "target_format": "json"
}'

# Staging con todo habilitado:
airflow dags trigger context_challenge --conf '{
  "environment": "staging",
  "data_source": "s3",
  "batch_size": 5000,
  "enable_validation": true,
  "enable_enrichment": true,
  "target_format": "parquet"
}'
"""

# TU CÓDIGO AQUÍ
# Implementa el DAG siguiendo las especificaciones anteriores
