"""
Challenge: El E-commerce Necesita un Pipeline Más Robusto

Trabajas en un e-commerce que procesa millones de dólares diarios. El pipeline actual de ETL falla
con frecuencia y cuando algo sale mal, nadie se entera hasta horas después. Tu lead te pide crear
un pipeline más profesional con manejo de errores inteligente.

Algunas tareas son rápidas y pueden reintentar muchas veces (como validaciones). Otras son pesadas
y consumen recursos (como calcular métricas complejas), así que deben reintentar menos. Las tareas
que consultan APIs externas necesitan más intentos porque la red puede fallar.

**Las tareas del pipeline:**

Empiezas con `inicio`. Luego validas en paralelo: `validar_conexiones` (a bases de datos y APIs,
debe ser super resiliente: 5 retries, timeout de 5 min) y `check_disk_space` (verifica que hay
espacio suficiente, timeout de 5 min). Checkpoint: `checkpoint1`.

Extraes datos en paralelo: `extraer_clientes`, `extraer_productos`, `extraer_ventas` (las tres
con 3 retries estándar, timeout de 5 min porque son consultas SQL). Checkpoint: `checkpoint2`.

Transformas: `transformar_clientes`, `transformar_productos`, `transformar_ventas` (usan defaults).
Luego `calcular_metricas_heavy` (proceso que tarda mucho: timeout de 2 horas, pero solo 1 retry
porque consume muchos recursos). Después `enriquecer_datos` (consulta API externa inestable: 
5 retries, retry_delay de solo 5 min para reintentar rápido). Checkpoint: `checkpoint3`.

Finalmente cargas: `cargar_warehouse` (usa defaults), `actualizar_cache` (no es crítico: 
email_on_failure=False, timeout de 5 min), `generar_reportes_ejecutivos` (super importante: 
envía a email=['data@', 'ceo@', 'cfo@'], email_on_failure=True). Terminas con `fin`.

**Default arguments del DAG:**
- owner: 'data_engineering_team'
- retries: 3
- retry_delay: 10 minutos
- execution_timeout: 30 minutos
- email: ['data@ecommerce.com']
- email_on_failure: True

**Configuración técnica:**
- DAG ID: `ecommerce_etl_pipeline`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'default_arguments']`
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Define defaults y el pipeline con overrides estratégicos
