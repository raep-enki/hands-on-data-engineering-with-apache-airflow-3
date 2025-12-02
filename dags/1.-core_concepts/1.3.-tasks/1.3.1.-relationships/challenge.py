"""
DESAFÍO: E-commerce Order Processing Pipeline

Crea un pipeline complejo de procesamiento de órdenes con múltiples
dependencias, paralelismo y convergencias.

CONTEXTO:
Eres Data Engineer de un e-commerce. Cada día debes procesar órdenes
que requieren validaciones, enrichment de múltiples fuentes, y carga
a diferentes sistemas.

REQUISITOS:

DAG: Order Processing Pipeline
- dag_id: 'task_relationships_challenge'
- schedule: '@daily'
- start_date: 7 días atrás
- catchup: False
- Tags: incluir 'challenge', 'tasks', 'relationships'

PIPELINE (mínimo 20 tareas):

**Stage 1: Inicio**
- start

**Stage 2: Validaciones Iniciales (3 en paralelo)**
- validate_schema
- validate_file_exists
- validate_business_date

**Stage 3: Checkpoint**
- checkpoint_validation (EmptyOperator)

**Stage 4: Extracciones (4 en paralelo)**
- extract_orders
- extract_customers
- extract_products
- extract_inventory

**Stage 5: Checkpoint**
- checkpoint_extract (EmptyOperator)

**Stage 6: Data Quality (3 en paralelo)**
- check_orders_quality
- check_customers_quality
- check_products_quality

**Stage 7: Enriquecimiento (dependencias complejas)**
- enrich_orders (necesita: orders + customers)
- enrich_products (necesita: products + inventory)

**Stage 8: Business Logic (dependencias complejas)**
- calculate_pricing (necesita: enrich_orders + enrich_products)
- calculate_shipping (necesita: enrich_orders)
- calculate_tax (necesita: enrich_orders)

**Stage 9: Agregaciones (en paralelo)**
- aggregate_daily_sales (necesita: calculate_pricing)
- aggregate_shipping_costs (necesita: calculate_shipping)
- aggregate_tax_revenue (necesita: calculate_tax)

**Stage 10: Checkpoint**
- checkpoint_aggregations (EmptyOperator)

**Stage 11: Cargas a sistemas (3 en paralelo)**
- load_to_warehouse (necesita: todas las agregaciones)
- load_to_reporting (necesita: todas las agregaciones)
- load_to_analytics (necesita: todas las agregaciones)

**Stage 12: Post-processing (2 en paralelo)**
- update_inventory (necesita: load_to_warehouse)
- send_notifications (necesita: load_to_reporting)

**Stage 13: Finalización**
- end (necesita: todos los post-processing)

RESTRICCIONES:
- Usar ÚNICAMENTE: BashOperator, EmptyOperator
- NO usar @task decorator (aún no se enseña)
- Cada bash_command debe ser descriptivo con echo
- Usar EmptyOperator para checkpoints entre stages
- Las dependencias deben reflejar lógica de negocio real

DEPENDENCIES ESPECÍFICAS:
```python
# Stage 2 → checkpoint_validation
[validate_schema, validate_file_exists, validate_business_date] >> checkpoint_validation

# checkpoint_validation → Stage 4
checkpoint_validation >> [extract_orders, extract_customers, extract_products, extract_inventory]

# Stage 4 → checkpoint_extract
[extract_orders, extract_customers, extract_products, extract_inventory] >> checkpoint_extract

# checkpoint_extract → Stage 6 (solo quality checks de datos extraídos)
checkpoint_extract >> [check_orders_quality, check_customers_quality, check_products_quality]

# Stage 6 → Stage 7 (enriquecimientos con dependencias específicas)
[check_orders_quality, check_customers_quality] >> enrich_orders
[check_products_quality] >> enrich_products  # inventory va directo sin quality check

# Stage 7 → Stage 8 (business logic)
[enrich_orders, enrich_products] >> calculate_pricing
enrich_orders >> calculate_shipping
enrich_orders >> calculate_tax

# Stage 8 → Stage 9 (agregaciones)
calculate_pricing >> aggregate_daily_sales
calculate_shipping >> aggregate_shipping_costs
calculate_tax >> aggregate_tax_revenue

# Stage 9 → checkpoint_aggregations
[aggregate_daily_sales, aggregate_shipping_costs, aggregate_tax_revenue] >> checkpoint_aggregations

# checkpoint_aggregations → Stage 11 (todas las cargas necesitan todas las agregaciones)
checkpoint_aggregations >> [load_to_warehouse, load_to_reporting, load_to_analytics]

# Stage 11 → Stage 12 (post-processing con dependencias específicas)
load_to_warehouse >> update_inventory
load_to_reporting >> send_notifications

# Stage 12 → end
[update_inventory, send_notifications] >> end
```

VALIDACIÓN:
En el Airflow UI, el grafo debe mostrar:
- 4 layers de paralelismo claro (validaciones, extracciones, quality checks, cargas)
- Checkpoints visibles separando stages
- Dependencies cruzadas entre enrichment y business logic
- Convergencia final en end

BASH COMMANDS:
Cada bash_command debe indicar:
- Qué hace la tarea
- De qué tareas depende (mencionar inputs)
- Qué produce (mencionar outputs)

Ejemplo:
```bash
bash_command='echo "⚙️ Enriqueciendo orders con customer data | Input: orders + customers | Output: enriched_orders"'
```

DOCUMENTACIÓN (doc_md):
Explicar:
1. Propósito del pipeline
2. Por qué las dependencias están configuradas así
3. Qué stages se ejecutan en paralelo y por qué
4. Qué checkpoints existen y su propósito
5. Orden de ejecución esperado
6. Tiempo estimado (con todo en paralelo vs secuencial)

TIPS:
- Usar chain() donde sea apropiado
- Agrupar dependencies por stage para legibilidad
- Nombrar tareas claramente (verbo + sustantivo)
- Checkpoints ayudan a visualizar el flujo
- No crear cycles (DAG debe ser acíclico)
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa el pipeline completo según los requisitos
# Recuerda: 20+ tareas, múltiples stages, paralelismo, dependencies complejas
