"""
Challenge: Sistema de Configuración Multi-Environment

Objetivo:
=========
Crear UN ÚNICO DAG que use Variables para configurar
un pipeline ETL que se comporta diferente según environment.

Requisitos:
===========

1. DAG: 'variables_challenge'
   - Schedule: @daily
   - Catchup: False
   - Tags: ['challenge', 'core_concepts', 'variables']

2. Variables requeridas (crear via CLI o asumir existen):
   - environment: 'development' | 'staging' | 'production'
   - batch_size: int (default: 1000)
   - enable_ml: 'true' | 'false' (default: 'false')
   - enable_notifications: 'true' | 'false' (default: 'true')
   - data_sources: JSON list de sources a procesar

3. Pipeline (6 tareas @task):

   A. load_configuration:
      - Lee TODAS las variables mencionadas
      - Aplica defaults apropiados
      - Valida valores (environment válido, batch_size > 0)
      - Return dict con configuración completa
   
   B. extract_data:
      - Recibe config
      - Extrae de cada source en data_sources
      - Batch size según config['batch_size']
      - Return: {'sources': list, 'total_records': int}
   
   C. validate_quality:
      - Recibe data
      - Si environment == 'development': skip validation (return data)
      - Si staging/production: run validation
      - Return: {'data': data, 'quality_score': int, 'passed': bool}
   
   D. conditional_ml_enrichment:
      - Recibe validated_data y config
      - Si config['enable_ml'] == True: enriquecer con ML
      - Si False: retornar sin cambios
      - Return: data (enriquecida o no)
   
   E. load_to_destination:
      - Recibe enriched_data y config
      - Destination basado en environment:
        * development: 'dev_database'
        * staging: 'staging_database'
        * production: 'prod_database'
      - Return: {'destination': str, 'records_loaded': int, 'status': 'success'}
   
   F. conditional_notify:
      - Recibe load_result y config
      - Si config['enable_notifications'] == True: enviar notificación
      - Si False: skip
      - Return: {'notified': bool, 'message': str}

4. Comportamiento por environment:
   - development: 
     * batch_size pequeño (100 si no especificado)
     * skip validation
     * ML opcional
   - staging:
     * batch_size medio (1000)
     * validation completa
     * ML para testing
   - production:
     * batch_size grande (10000)
     * validation estricta
     * ML según flag

5. data_sources JSON ejemplo:
   ```json
   ["api", "database", "s3"]
   ```

Restricciones:
==============
- Usar @task decorator
- Variable.get() con defaults siempre
- Validar valores leídos
- Lógica condicional basada en variables
- Print estado de cada config leída

Tips:
=====
- Variable.get("key", default_var="value")
- Variable.get("json_key", default_var='[]', deserialize_json=True)
- bool_val = Variable.get("flag", default_var="false") == "true"
- Validar: if env not in ['development', 'staging', 'production']

Evaluación:
===========
- ✅ Lectura correcta de variables con defaults
- ✅ Validación de valores
- ✅ Comportamiento diferenciado por environment
- ✅ Feature flags funcionando
- ✅ JSON variables deserializadas
- ✅ Lógica condicional apropiada

Setup de ejemplo:
=================
```bash
airflow variables set environment development
airflow variables set batch_size 100
airflow variables set enable_ml false
airflow variables set enable_notifications true
airflow variables set data_sources '["api", "database"]'
```
"""

# TU CÓDIGO AQUÍ
