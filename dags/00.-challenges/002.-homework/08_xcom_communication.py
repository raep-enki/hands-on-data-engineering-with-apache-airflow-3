"""
HOMEWORK 08 - Comunicación con XCom (Nivel: Medio)
==================================================

OBJETIVO:
Usar XCom para compartir múltiples valores entre tareas.

REQUISITOS:
1. Crear un DAG llamado 'homework_08_xcom'
2. Crear función @task 'extract_sales_data' que retorne:
   {
       'daily_sales': [120, 150, 180, 200, 175],
       'region': 'North',
       'currency': 'USD'
   }
3. Crear función @task 'extract_inventory_data' que retorne:
   {
       'stock_levels': [50, 45, 40, 35, 30],
       'warehouse': 'Main'
   }
4. Crear función @task 'calculate_metrics' que:
   - Reciba sales_data e inventory_data
   - Calcule:
     * total_sales = sum de daily_sales
     * avg_sales = promedio de daily_sales
     * min_stock = mínimo de stock_levels
   - Retorne dict con las métricas
5. Crear función @task 'generate_report' que:
   - Reciba sales_data, inventory_data, y metrics
   - Genere un reporte formateado mostrando:
     * Region y warehouse
     * Total y promedio de ventas
     * Stock mínimo
     * Alert si stock mínimo < 40
6. Flujo: [extract_sales, extract_inventory] >> calculate >> generate_report
7. Configurar schedule='@daily', start_date enero 2021, catchup=False
8. Agregar tags: ['homework', 'nivel_03', 'xcom']

CONCEPTOS:
- XCom para comunicación entre tareas
- TaskFlow API y paso automático de datos
- Multiple inputs en una tarea
- Agregación de datos de múltiples fuentes

RESULTADO ESPERADO:
Datos de múltiples fuentes se combinan y procesan para generar reporte.
"""

import datetime

from airflow.sdk import DAG, task

# TODO: Implementa tu solución aquí
