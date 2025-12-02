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

# Solución del challenge
default_args = {
    'owner': 'data_engineering_team',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
    'email': ['data@ecommerce.com'],
    'email_on_failure': True,
}

with DAG(
    dag_id='ecommerce_etl_pipeline',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    tags=['challenge', 'default_arguments'],
) as dag:
    
    inicio = EmptyOperator(task_id='inicio')
    
    validar_conexiones = BashOperator(
        task_id='validar_conexiones',
        bash_command='echo "Validando conexiones..."',
        retries=5,
        execution_timeout=timedelta(minutes=5),
    )
    
    check_disk_space = BashOperator(
        task_id='check_disk_space',
        bash_command='echo "Espacio disponible: 500GB"',
        execution_timeout=timedelta(minutes=5),
    )
    
    checkpoint1 = EmptyOperator(task_id='checkpoint1')
    
    extraer_clientes = BashOperator(
        task_id='extraer_clientes',
        bash_command='echo "Extrayendo clientes..."',
        execution_timeout=timedelta(minutes=5),
    )
    
    extraer_productos = BashOperator(
        task_id='extraer_productos',
        bash_command='echo "Extrayendo productos..."',
        execution_timeout=timedelta(minutes=5),
    )
    
    extraer_ventas = BashOperator(
        task_id='extraer_ventas',
        bash_command='echo "Extrayendo ventas..."',
        execution_timeout=timedelta(minutes=5),
    )
    
    checkpoint2 = EmptyOperator(task_id='checkpoint2')
    
    transformar_clientes = BashOperator(
        task_id='transformar_clientes',
        bash_command='echo "Transformando clientes..."',
    )
    
    transformar_productos = BashOperator(
        task_id='transformar_productos',
        bash_command='echo "Transformando productos..."',
    )
    
    transformar_ventas = BashOperator(
        task_id='transformar_ventas',
        bash_command='echo "Transformando ventas..."',
    )
    
    calcular_metricas_heavy = BashOperator(
        task_id='calcular_metricas_heavy',
        bash_command='echo "Calculando métricas pesadas..."',
        execution_timeout=timedelta(hours=2),
        retries=1,
    )
    
    enriquecer_datos = BashOperator(
        task_id='enriquecer_datos',
        bash_command='echo "Enriqueciendo con API externa..."',
        retries=5,
        retry_delay=timedelta(minutes=5),
    )
    
    checkpoint3 = EmptyOperator(task_id='checkpoint3')
    
    cargar_warehouse = BashOperator(
        task_id='cargar_warehouse',
        bash_command='echo "Cargando a warehouse..."',
    )
    
    actualizar_cache = BashOperator(
        task_id='actualizar_cache',
        bash_command='echo "Actualizando cache..."',
        email_on_failure=False,
        execution_timeout=timedelta(minutes=5),
    )
    
    generar_reportes_ejecutivos = BashOperator(
        task_id='generar_reportes_ejecutivos',
        bash_command='echo "Generando reportes ejecutivos..."',
        email=['data@ecommerce.com', 'ceo@ecommerce.com', 'cfo@ecommerce.com'],
        email_on_failure=True,
    )
    
    fin = EmptyOperator(task_id='fin')
    
    # Dependencies
    inicio >> [validar_conexiones, check_disk_space] >> checkpoint1
    checkpoint1 >> [extraer_clientes, extraer_productos, extraer_ventas] >> checkpoint2
    checkpoint2 >> transformar_clientes >> calcular_metricas_heavy
    checkpoint2 >> transformar_productos >> calcular_metricas_heavy
    checkpoint2 >> transformar_ventas >> calcular_metricas_heavy
    calcular_metricas_heavy >> enriquecer_datos >> checkpoint3
    checkpoint3 >> [cargar_warehouse, actualizar_cache, generar_reportes_ejecutivos] >> fin
