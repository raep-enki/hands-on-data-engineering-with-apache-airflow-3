"""
Challenge: El Pipeline de E-commerce con Muchas Dependencias

Tu empresa procesa órdenes de e-commerce y el flujo es complejo con múltiples fuentes de datos
que deben coordinarse. Algunas tareas esperan una entrada, otras esperan múltiples. Algunas
corren en paralelo, otras convergen. Debes demostrar dominio total de `>>` y listas de dependencias.

**El flujo completo (20+ tareas con dependencias cruzadas):**

`start` (EmptyOperator) se ramifica en 4 validaciones paralelas:
- `validate_orders_file` (BashOperator - verifica que orders.csv existe y tiene formato correcto)
- `validate_customers_file` (BashOperator - verifica customers.csv)
- `validate_products_file` (BashOperator - verifica products.csv)
- `validate_inventory_file` (BashOperator - verifica inventory.csv)

**Extracción paralela** (cada validación lleva a su extracción):
- `validate_orders_file` >> `extract_orders` (BashOperator - parsea CSV a JSON)
- `validate_customers_file` >> `extract_customers` (BashOperator - parsea CSV)
- `validate_products_file` >> `extract_products` (BashOperator - parsea CSV)
- `validate_inventory_file` >> `extract_inventory` (BashOperator - parsea CSV)

**Quality checks paralelos** (cada extracción debe pasar quality):
- `extract_orders` >> `quality_check_orders` (BashOperator - valida fechas, montos positivos)
- `extract_customers` >> `quality_check_customers` (BashOperator - valida emails, addresses)
- `extract_products` >> `quality_check_products` (BashOperator - valida SKUs únicos)
- `extract_inventory` >> `quality_check_inventory` (BashOperator - valida stock >= 0)

**Enriquecimiento** (dependencias cruzadas aquí):
- `[quality_check_orders, quality_check_customers]` >> `enrich_orders_with_customer_data` 
  (BashOperator - hace join de orders con customer info: nombre, dirección, historial)
- `[quality_check_products, quality_check_inventory]` >> `enrich_products_with_inventory` 
  (BashOperator - hace join de products con inventory: stock disponible, warehouse location)

**Cálculos que dependen de múltiples inputs:**
- `[enrich_orders_with_customer_data, enrich_products_with_inventory]` >> `calculate_pricing`
  (BashOperator - calcula precio final considerando descuentos de cliente Y disponibilidad de producto)
- `enrich_orders_with_customer_data` >> `calculate_shipping` 
  (BashOperator - calcula costo de envío basado solo en dirección del cliente)
- `enrich_orders_with_customer_data` >> `calculate_taxes` 
  (BashOperator - calcula impuestos basado en ubicación del cliente)

**Agregación** (todo converge):
- `[calculate_pricing, calculate_shipping, calculate_taxes]` >> `aggregate_order_totals`
  (BashOperator - suma pricing + shipping + taxes para obtener total final de cada orden)

`aggregate_order_totals` >> `generate_invoice` (BashOperator - genera PDF de factura) >>
`generate_business_report` (BashOperator - genera reporte ejecutivo con métricas del día)

**Carga paralela a múltiples destinos:**
`generate_business_report` se ramifica a 4 cargas paralelas:
- `load_to_data_warehouse` (BashOperator - inserta en Snowflake para analytics)
- `load_to_operational_db` (BashOperator - actualiza PostgreSQL de producción)
- `send_to_s3` (BashOperator - backup en S3 para compliance)
- `trigger_email_notifications` (BashOperator - envía resumen a stakeholders)

Todas convergen en `end` (EmptyOperator con `trigger_rule='none_failed'` - solo termina si nada falló).

**Configuración técnica:**
- DAG ID: `task_relationships_challenge`
- Schedule: @daily
- Start date: 7 días atrás desde hoy
- Catchup: False
- Tags: `['challenge', 'relationships']`
- Total: 25 tareas con dependencias de 1:1, 1:N, N:1, y N:M
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Diseña el flujo con dependencias complejas
