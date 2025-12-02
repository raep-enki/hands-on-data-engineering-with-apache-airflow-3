"""
Challenge: Pipeline Configurable con Params

Objetivo:
=========
Crear UN ÚNICO DAG altamente configurable usando params
para controlar todo el comportamiento del pipeline.

Requisitos:
===========

1. DAG: 'params_challenge'
   - Schedule: @daily
   - Catchup: False
   - Tags: ['challenge', 'core_concepts', 'params']

2. Params requeridos:
   ```python
   params={
       'data_sources': ['api', 'database'],  # Lista de sources
       'processing_config': {  # Config nested
           'batch_size': 1000,
           'parallel': True,
           'timeout_seconds': 300
       },
       'quality_config': {
           'enable_validation': True,
           'min_quality_score': 80,
           'fail_on_low_quality': False
       },
       'output_config': {
           'format': 'parquet',  # 'parquet', 'json', 'csv'
           'destination': 'warehouse',
           'partition_by_date': True
       },
       'feature_flags': {
           'enable_ml': False,
           'enable_deduplication': True,
           'enable_notifications': True
       }
   }
   ```

3. Pipeline (7 tareas @task):

   A. validate_configuration:
      - Lee todos los params
      - Valida:
        * data_sources no vacío
        * batch_size > 0
        * min_quality_score entre 0-100
        * format en ['parquet', 'json', 'csv']
      - Si inválido: raise ValueError
      - Return: dict con config validada
   
   B. extract_from_sources:
      - Extrae de cada source en params['data_sources']
      - Usa params['processing_config']['batch_size']
      - Return: {'records': list, 'count': int, 'sources': list}
   
   C. conditional_deduplication:
      - Si params['feature_flags']['enable_deduplication']: dedup
      - Si no: skip
      - Return: data (dedup o original)
   
   D. quality_check:
      - Si params['quality_config']['enable_validation']: validate
      - Calcula quality_score (0-100)
      - Si score < min_quality_score y fail_on_low_quality: raise
      - Return: {'data': data, 'quality_score': int, 'passed': bool}
   
   E. conditional_ml_enrichment:
      - Si params['feature_flags']['enable_ml']: enriquecer
      - Si no: skip
      - Return: data (enriquecida o no)
   
   F. format_and_partition:
      - Formatea según params['output_config']['format']
      - Si partition_by_date: crea particiones year/month/day
      - Return: {'formatted_data': data, 'partitions': list, 'format': str}
   
   G. load_and_notify:
      - Carga a params['output_config']['destination']
      - Si params['feature_flags']['enable_notifications']: notify
      - Return: {'status': 'success', 'destination': str, 'records_loaded': int}

4. Dependencias lineales:
   validate → extract → dedup → quality → ml → format → load

5. Comportamiento:
   - Todos los params deben ser accesibles y usados
   - Validación early (primera tarea)
   - Skip features si flags = False
   - Fail si quality check falla y fail_on_low_quality = True

Restricciones:
==============
- Usar @task decorator
- Acceder params via context['params']
- Navegar objetos nested: params['config']['key']
- Lógica condicional basada en flags
- Validación de todos los params críticos

Tips:
=====
- params = context['params']
- config = params['processing_config']
- batch_size = config['batch_size']
- for source in params['data_sources']: ...
- if params['feature_flags']['enable_x']: ...

Evaluación:
===========
- ✅ Validación completa de params
- ✅ Navegación de nested objects
- ✅ Iteración sobre listas
- ✅ Conditional logic con flags
- ✅ Todos los params usados apropiadamente
- ✅ Error handling si validation falla

Trigger de ejemplo:
===================
```bash
# Default (todos los flags)
airflow dags trigger params_challenge

# Custom: solo validation, sin ML
airflow dags trigger params_challenge --conf '{
  "data_sources": ["api"],
  "processing_config": {"batch_size": 500},
  "quality_config": {"enable_validation": true},
  "feature_flags": {
    "enable_ml": false,
    "enable_deduplication": true,
    "enable_notifications": false
  }
}'
```
"""

# TU CÓDIGO AQUÍ
