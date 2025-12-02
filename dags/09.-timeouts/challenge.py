"""
Challenge: Sistema de Monitoreo con Timeouts y SLAs

Objetivo:
=========
Crear UN ÚNICO DAG que implemente un sistema de procesamiento de datos
con diferentes niveles de criticidad y estrategias de timeout.

Requisitos:
===========

1. DAG: 'timeout_challenge'
   - Schedule: Cada 30 minutos
   - Catchup: False
   - Tags: ['challenge', 'core_concepts', 'tasks', 'timeouts']

2. Estructura del pipeline:
   
   A. STAGE 1 - Data Collection (2 fuentes paralelas):
      - collect_api_data:
        * Timeout: 2 minutos
        * Retries: 3
        * Retry delay: 30 segundos
        * SLA: 1 minuto
      
      - collect_database_data:
        * Timeout: 5 minutos
        * Retries: 2
        * Retry delay: 1 minuto
        * SLA: 3 minutos
   
   B. STAGE 2 - Data Validation:
      - validate_data:
        * Sin timeout explícito (usa default_args)
        * SLA: 2 minutos
   
   C. STAGE 3 - Processing (3 tareas paralelas con diferentes criticidades):
      - fast_processing (crítico):
        * Timeout: 5 minutos
        * Retries: 1
        * SLA: 3 minutos
      
      - medium_processing (moderado):
        * Timeout: 15 minutos
        * Retries: 2
        * SLA: 10 minutos
      
      - slow_processing (batch, no crítico):
        * Timeout: 1 hora
        * Retries: 1
        * SLA: 45 minutos
   
   D. STAGE 4 - Quality Check (sensor):
      - wait_for_quality_threshold:
        * Sensor que espera que la calidad supere 90%
        * Poke interval: 30 segundos
        * Timeout: 5 minutos (300 segundos)
        * Mode: poke
   
   E. STAGE 5 - Final Load:
      - load_to_warehouse:
        * Timeout: 10 minutos
        * Retries: 2
        * Retry delay: 2 minutos
        * SLA: 7 minutos

3. Default Args:
   - execution_timeout: 10 minutos (para tareas sin timeout explícito)
   - retries: 1
   - retry_delay: 1 minuto

4. SLA Callback:
   - Implementar función sla_miss_callback que:
     * Imprima mensaje de alerta
     * Liste las tareas que perdieron SLA
     * Incluya timestamp del miss

5. Comandos Bash:
   - Simular ejecución con echo y sleep
   - Incluir emojis para visualizar etapas
   - Los sleep deben ser < 5 segundos (es simulación)

6. Dependencias:
   - collect_api_data, collect_database_data → validate_data
   - validate_data → fast_processing, medium_processing, slow_processing
   - fast_processing, medium_processing, slow_processing → wait_for_quality_threshold
   - wait_for_quality_threshold → load_to_warehouse

7. Documentación:
   - DAG doc_md explicando:
     * Estrategia de timeouts por nivel de criticidad
     * Por qué algunos tienen SLA y otros no
     * Estrategia de retries
     * Cómo interpretar las alertas SLA

Restricciones:
==============
- NO usar TaskFlow API (@task decorator)
- Solo usar: BashOperator, EmptyOperator, BashSensor
- Un solo DAG en el archivo
- No usar XComs, Variables, ni Params

Tips:
=====
- Los timeouts de tareas críticas deben ser más cortos
- SLA debe ser menor que timeout
- Retries más altos para tareas propensas a fallos temporales
- Sensor timeout debe considerar frecuencia de poke
- Default args establece baseline, tareas críticas override

Evaluación:
===========
- ✅ Timeouts apropiados por criticidad
- ✅ SLAs menores que timeouts
- ✅ Retries balanceados
- ✅ Sensor correctamente configurado
- ✅ Default args aplicados correctamente
- ✅ Callback SLA implementado
- ✅ Documentación clara
"""

# TU CÓDIGO AQUÍ
# Implementa el DAG siguiendo las especificaciones anteriores
