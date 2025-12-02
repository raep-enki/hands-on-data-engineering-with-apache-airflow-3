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

# Solución del challenge
with DAG(
    dag_id='task_relationships_challenge',
    start_date=datetime.datetime.now() - datetime.timedelta(days=7),
    schedule='@daily',
    catchup=False,
    tags=['challenge', 'relationships'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # 4 validaciones paralelas
    validate_orders_file = BashOperator(task_id='validate_orders_file', bash_command='echo "Validating orders.csv"')
    validate_customers_file = BashOperator(task_id='validate_customers_file', bash_command='echo "Validating customers.csv"')
    validate_products_file = BashOperator(task_id='validate_products_file', bash_command='echo "Validating products.csv"')
    validate_inventory_file = BashOperator(task_id='validate_inventory_file', bash_command='echo "Validating inventory.csv"')
    
    # Extracciones paralelas (dependencia 1:1 con validaciones)
    extract_orders = BashOperator(task_id='extract_orders', bash_command='echo "Extracting orders to JSON"')
    extract_customers = BashOperator(task_id='extract_customers', bash_command='echo "Extracting customers to JSON"')
    extract_products = BashOperator(task_id='extract_products', bash_command='echo "Extracting products to JSON"')
    extract_inventory = BashOperator(task_id='extract_inventory', bash_command='echo "Extracting inventory to JSON"')
    
    # Quality checks paralelos (dependencia 1:1 con extracciones)
    quality_check_orders = BashOperator(task_id='quality_check_orders', bash_command='echo "QC: Orders validated"')
    quality_check_customers = BashOperator(task_id='quality_check_customers', bash_command='echo "QC: Customers validated"')
    quality_check_products = BashOperator(task_id='quality_check_products', bash_command='echo "QC: Products validated"')
    quality_check_inventory = BashOperator(task_id='quality_check_inventory', bash_command='echo "QC: Inventory validated"')
    
    # Enriquecimiento (dependencias cruzadas N:1)
    enrich_orders_with_customer_data = BashOperator(
        task_id='enrich_orders_with_customer_data',
        bash_command='echo "Enriching orders with customer data"'
    )
    enrich_products_with_inventory = BashOperator(
        task_id='enrich_products_with_inventory',
        bash_command='echo "Enriching products with inventory data"'
    )
    
    # Cálculos (dependencias múltiples)
    calculate_pricing = BashOperator(
        task_id='calculate_pricing',
        bash_command='echo "Calculating pricing with discounts and availability"'
    )
    calculate_shipping = BashOperator(
        task_id='calculate_shipping',
        bash_command='echo "Calculating shipping costs"'
    )
    calculate_taxes = BashOperator(
        task_id='calculate_taxes',
        bash_command='echo "Calculating taxes"'
    )
    
    # Agregación (dependencias N:1)
    aggregate_order_totals = BashOperator(
        task_id='aggregate_order_totals',
        bash_command='echo "Aggregating order totals"'
    )
    
    # Generación de reportes
    generate_invoice = BashOperator(
        task_id='generate_invoice',
        bash_command='echo "Generating PDF invoice"'
    )
    generate_business_report = BashOperator(
        task_id='generate_business_report',
        bash_command='echo "Generating business report"'
    )
    
    # Carga paralela a múltiples destinos (dependencias 1:N)
    load_to_data_warehouse = BashOperator(
        task_id='load_to_data_warehouse',
        bash_command='echo "Loading to Snowflake"'
    )
    load_to_operational_db = BashOperator(
        task_id='load_to_operational_db',
        bash_command='echo "Loading to PostgreSQL"'
    )
    send_to_s3 = BashOperator(
        task_id='send_to_s3',
        bash_command='echo "Backing up to S3"'
    )
    trigger_email_notifications = BashOperator(
        task_id='trigger_email_notifications',
        bash_command='echo "Sending notifications"'
    )
    
    end = EmptyOperator(task_id='end', trigger_rule='none_failed')
    
    # Dependencies: start ramifica a validaciones
    start >> [validate_orders_file, validate_customers_file, validate_products_file, validate_inventory_file]
    
    # Validaciones a extracciones (1:1)
    validate_orders_file >> extract_orders
    validate_customers_file >> extract_customers
    validate_products_file >> extract_products
    validate_inventory_file >> extract_inventory
    
    # Extracciones a quality checks (1:1)
    extract_orders >> quality_check_orders
    extract_customers >> quality_check_customers
    extract_products >> quality_check_products
    extract_inventory >> quality_check_inventory
    
    # Quality checks a enriquecimiento (N:1 - dependencias cruzadas)
    [quality_check_orders, quality_check_customers] >> enrich_orders_with_customer_data
    [quality_check_products, quality_check_inventory] >> enrich_products_with_inventory
    
    # Enriquecimiento a cálculos (N:M)
    [enrich_orders_with_customer_data, enrich_products_with_inventory] >> calculate_pricing
    enrich_orders_with_customer_data >> calculate_shipping
    enrich_orders_with_customer_data >> calculate_taxes
    
    # Cálculos a agregación (N:1)
    [calculate_pricing, calculate_shipping, calculate_taxes] >> aggregate_order_totals
    
    # Agregación a reportes (1:1)
    aggregate_order_totals >> generate_invoice >> generate_business_report
    
    # Reportes a cargas paralelas (1:N)
    generate_business_report >> [load_to_data_warehouse, load_to_operational_db, send_to_s3, trigger_email_notifications]
    
    # Todas las cargas convergen en end (N:1)
    [load_to_data_warehouse, load_to_operational_db, send_to_s3, trigger_email_notifications] >> end
