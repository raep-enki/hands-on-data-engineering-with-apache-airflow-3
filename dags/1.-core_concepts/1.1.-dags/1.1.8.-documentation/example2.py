"""
# ETL Pipeline - Sales Data

## Propósito
Este DAG extrae, transforma y carga datos de ventas diarias desde
múltiples fuentes al data warehouse corporativo.

## Schedule
Se ejecuta diariamente a las 2 AM UTC para procesar las ventas del día anterior.

## Dependencias
- Base de datos PostgreSQL (ventas)
- API de CRM (datos de clientes)
- S3 bucket (archivos de productos)

## Contacto
Para problemas o preguntas, contactar al equipo de Data Engineering.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='documentation_detailed',
    schedule='0 2 * * *',  # 2 AM UTC
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'documentation']
) as dag:
    dag.doc_md = __doc__
    
    start = EmptyOperator(task_id='start')
    
    extract_sales = BashOperator(
        task_id='extract_sales_data',
        bash_command='echo "📥 Extrayendo ventas de PostgreSQL"'
    )
    extract_sales.doc_md = """
    ### Extract Sales Data
    
    Extrae registros de ventas de la base de datos transaccional.
    
    **Conexión**: `postgres_sales_db`  
    **Tabla**: `public.sales`  
    **Filtro**: `sale_date = {{ ds }}`  
    **Volumen estimado**: ~100K registros/día
    
    #### Campos extraídos
    - sale_id (PK)
    - customer_id (FK)
    - product_id (FK)
    - amount (decimal)
    - sale_date (date)
    - created_at (timestamp)
    """
    
    extract_customers = BashOperator(
        task_id='extract_customer_data',
        bash_command='echo "📥 Extrayendo clientes de CRM API"'
    )
    extract_customers.doc_md = """
    ### Extract Customer Data
    
    Obtiene información actualizada de clientes desde la API del CRM.
    
    **Endpoint**: `https://api.crm.company.com/v2/customers`  
    **Autenticación**: OAuth 2.0 (token renovado cada 1h)  
    **Rate limit**: 1000 requests/minuto  
    **Retry policy**: 3 intentos con backoff exponencial
    
    #### Datos obtenidos
    - customer_id
    - name, email, phone
    - segment (gold/silver/bronze)
    - lifetime_value
    """
    
    transform = BashOperator(
        task_id='transform_and_enrich',
        bash_command='echo "⚙️ Transformando y enriqueciendo datos"'
    )
    transform.doc_md = """
    ### Transform and Enrich
    
    Aplica transformaciones y enriquece los datos con información adicional.
    
    #### Transformaciones aplicadas
    1. **Limpieza**
       - Remover duplicados basado en (sale_id, sale_date)
       - Eliminar registros con amount < 0
       - Validar customer_id y product_id existen
    
    2. **Enriquecimiento**
       - Agregar información de customer segment
       - Calcular descuentos aplicados
       - Agregar categoría de producto
    
    3. **Agregaciones**
       - Total de ventas por cliente
       - Total de ventas por producto
       - Promedio de ticket por segmento
    
    **Tiempo de ejecución**: ~15 minutos para 100K registros
    """
    
    load = BashOperator(
        task_id='load_to_warehouse',
        bash_command='echo "📤 Cargando a data warehouse"'
    )
    load.doc_md = """
    ### Load to Data Warehouse
    
    Carga los datos transformados al data warehouse en Snowflake.
    
    **Conexión**: `snowflake_dwh`  
    **Schema**: `analytics.sales`  
    **Tabla**: `fact_daily_sales`  
    **Método**: MERGE (upsert basado en sale_id)
    
    #### Particionamiento
    - Por sale_date (diario)
    - Por region_id
    
    #### Índices
    - PK: (sale_id, sale_date)
    - FK: customer_id, product_id
    - Índice: sale_date, region_id
    
    **SLA**: Debe completarse antes de las 6 AM para reportes matutinos
    """
    
    end = EmptyOperator(task_id='end')
    
    start >> [extract_sales, extract_customers] >> transform >> load >> end
