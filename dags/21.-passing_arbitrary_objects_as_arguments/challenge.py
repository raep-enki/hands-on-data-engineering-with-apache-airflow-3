"""
Challenge: Pipeline ETL Complejo con TaskFlow

Objetivo:
=========
Crear UN ÚNICO DAG que procese datos de múltiples fuentes,
los combine, enriquezca, valide y cargue usando TaskFlow API.

Requisitos:
===========

1. DAG: 'passing_objects_challenge'
   - Schedule: @daily
   - Catchup: False
   - Tags: ['challenge', 'core_concepts', 'task_flow', 'passing_arbitrary_objects_as_arguments']

2. Pipeline (8 tareas @task):

   A. extract_users:
      - Retorna list de 5 users:
        [{'id': int, 'name': str, 'age': int, 'country': str}, ...]
   
   B. extract_orders:
      - Retorna list de 10 orders:
        [{'order_id': int, 'user_id': int, 'amount': float, 'date': str}, ...]
   
   C. validate_users:
      - Recibe users list
      - Valida que todos tengan age >= 18
      - Retorna dict: {'valid_users': list, 'invalid_count': int}
   
   D. validate_orders:
      - Recibe orders list
      - Valida que todos tengan amount > 0
      - Retorna dict: {'valid_orders': list, 'invalid_count': int}
   
   E. enrich_orders_with_users:
      - Recibe validated_users dict y validated_orders dict
      - Para cada order, agrega info del user (name, country)
      - Retorna list de enriched_orders:
        [{'order_id', 'user_id', 'amount', 'date', 'user_name', 'user_country'}, ...]
   
   F. calculate_aggregates:
      - Recibe enriched_orders
      - Calcula:
        * total_amount: suma de todos amounts
        * order_count: número de orders
        * avg_amount: promedio
        * orders_by_country: dict {country: count}
      - Retorna dict con estas stats
   
   G. generate_summary_report:
      - Recibe enriched_orders y aggregates
      - Retorna dict con reporte completo:
        * top_countries: top 3 countries por order count
        * high_value_orders: orders con amount > avg_amount
        * summary_stats: aggregates completos
   
   H. validate_and_load:
      - Recibe summary_report
      - Valida que hay al menos 1 high_value_order
      - Valida que total_amount > 0
      - Si válido, retorna {'status': 'loaded', 'report': summary_report}
      - Si inválido, raise ValueError

3. Dependencias:
   - extract_users >> validate_users >> enrich_orders_with_users
   - extract_orders >> validate_orders >> enrich_orders_with_users
   - enrich_orders_with_users >> calculate_aggregates
   - enrich_orders_with_users >> generate_summary_report (también recibe aggregates)
   - calculate_aggregates >> generate_summary_report
   - generate_summary_report >> validate_and_load

4. Datos de ejemplo:
   - Users: 5 users con ids 1-5, ages 20-40, countries: 'US', 'UK', 'DE'
   - Orders: 10 orders con user_ids aleatorios (1-5), amounts 50-500

5. Type hints obligatorios en todas las funciones

Restricciones:
==============
- Solo @task decorator
- No usar EmptyOperator
- Type hints en todas las funciones
- Prints con emojis en cada tarea
- Manejo correcto de estructuras nested

Evaluación:
===========
- ✅ Extracción de múltiples fuentes
- ✅ Validaciones correctas
- ✅ Join/enriquecimiento de datos
- ✅ Agregaciones calculadas
- ✅ Fan-in correcto (múltiples → una tarea)
- ✅ Type hints apropiados
- ✅ Validación final con assertions
"""

# TU CÓDIGO AQUÍ
