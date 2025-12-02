"""
Challenge: Pipelines Que Se Disparan Por Datos, No Por Tiempo

Normalmente los DAGs corren por schedule: "cada hora", "todos los días a las 2 AM". Pero a veces
quieres que un DAG corra cuando OTRO DAG termine de generar datos. No es tiempo, es dependencia
de datos. Es event-driven, no time-driven.

Assets (antes llamados Datasets) permiten esto: un DAG "produce" un asset, otro lo "consume".
Cuando se produce, el consumidor se dispara automáticamente. Es como dependency graph entre DAGs.

Crea 3 DAGs: uno produce raw data, otro consume raw y produce processed, un tercero consume processed
para analytics. Tags: `['challenge', 'asset_definitions']`.
"""

import datetime

from airflow.sdk import DAG, task, Asset

# Solución del challenge

# Definir Assets (datos que conectan DAGs)
raw_sales_data = Asset('s3://data-lake/raw/sales.parquet')
processed_sales_data = Asset('s3://data-lake/processed/sales.parquet')

# DAG 1: Produce raw data
with DAG(
    dag_id='asset_producer_raw',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False,
    tags=['challenge', 'asset_definitions', 'producer'],
) as dag1:
    
    @task(outlets=[raw_sales_data])  # Declara que PRODUCE este asset
    def extract_raw_sales(**context):
        """Extrae datos crudos y los guarda"""
        print("Extracting raw sales from source system")
        print(f"Saving to {raw_sales_data.uri}")
        
        # Simulación: guardaría en S3
        records = 1000
        print(f"Saved {records} raw records")
        
        return {'records': records, 'location': raw_sales_data.uri}
    
    extract_raw_sales()

# DAG 2: Consume raw, produce processed
with DAG(
    dag_id='asset_consumer_processor',
    start_date=datetime.datetime(2024, 1, 1),
    schedule=[raw_sales_data],  # Se dispara cuando raw_sales_data está listo
    catchup=False,
    tags=['challenge', 'asset_definitions', 'consumer', 'producer'],
) as dag2:
    
    @task
    def read_raw_data(**context):
        """Lee el raw data que disparó este DAG"""
        print(f"Reading from {raw_sales_data.uri}")
        # En producción: leería de S3
        return {'records': 1000}
    
    @task
    def clean_and_transform(raw, **context):
        """Limpia y transforma datos"""
        print(f"Cleaning {raw['records']} records")
        clean_records = int(raw['records'] * 0.95)  # 5% descartados
        return {'records': clean_records}
    
    @task(outlets=[processed_sales_data])  # Declara que PRODUCE processed
    def save_processed(cleaned, **context):
        """Guarda datos procesados"""
        print(f"Saving {cleaned['records']} processed records")
        print(f"Location: {processed_sales_data.uri}")
        return {'records': cleaned['records'], 'location': processed_sales_data.uri}
    
    raw = read_raw_data()
    cleaned = clean_and_transform(raw)
    save_processed(cleaned)

# DAG 3: Consume processed para analytics
with DAG(
    dag_id='asset_consumer_analytics',
    start_date=datetime.datetime(2024, 1, 1),
    schedule=[processed_sales_data],  # Se dispara cuando processed está listo
    catchup=False,
    tags=['challenge', 'asset_definitions', 'consumer'],
) as dag3:
    
    @task
    def load_processed_data(**context):
        """Carga datos procesados para analytics"""
        print(f"Loading from {processed_sales_data.uri}")
        return {'records': 950}
    
    @task
    def calculate_kpis(data, **context):
        """Calcula KPIs de negocio"""
        records = data['records']
        print(f"Calculating KPIs for {records} records")
        
        # Simulación de KPIs
        revenue = records * 125.50
        avg_order = revenue / records
        
        print(f"Total revenue: ${revenue:,.2f}")
        print(f"Average order: ${avg_order:.2f}")
        
        return {'revenue': revenue, 'avg_order': avg_order, 'orders': records}
    
    @task
    def send_analytics_report(kpis, **context):
        """Envía reporte de analytics"""
        print("=== SALES ANALYTICS REPORT ===")
        print(f"Orders: {kpis['orders']}")
        print(f"Revenue: ${kpis['revenue']:,.2f}")
        print(f"Avg Order Value: ${kpis['avg_order']:.2f}")
        return {'status': 'sent'}
    
    data = load_processed_data()
    kpis = calculate_kpis(data)
    send_analytics_report(kpis)
