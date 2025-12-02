"""
Challenge: Configuración Que No Vive en el Código

Tu pipeline necesita conectarse a bases de datos. Los hosts y puertos cambian entre desarrollo,
staging y producción. Las API keys son secretas. Los batch sizes varían. Las configuraciones de
email también. NO quieres hardcodear esto en tu código.

La solución: Airflow Variables. Las guardas en la UI de Airflow (o las importas de archivos),
y tu código las lee. Cambias un valor en la UI, y todos los DAGs usan el nuevo valor sin cambiar código.

Tu pipeline debe leer el environment (dev/staging/prod) y según eso, usar diferentes hosts, puertos,
API keys. Debe respetar batch sizes configurables, saber si enviar notificaciones o no, cuántos
retries intentar.

Crea el DAG `variables_challenge` diario, sin catchup, usando @task que lean Variables de Airflow
para configuración multi-environment. Tags: `['challenge', 'variables']`.
"""

import datetime

from airflow.sdk import DAG, task, Variable

# Solución del challenge

@task
def get_environment_config(**context):
    """Lee configuración según environment"""
    # Leer environment de Variable (default: development)
    env = Variable.get("environment", default="development")
    
    print(f"Running in {env} environment")
    
    # Configuración según environment
    if env == "production":
        db_host = Variable.get("prod_db_host", default="prod.db.company.com")
        db_port = Variable.get("prod_db_port", default="5432")
        api_key = Variable.get("prod_api_key", default="secret_prod_key")
    elif env == "staging":
        db_host = Variable.get("staging_db_host", default="staging.db.company.com")
        db_port = Variable.get("staging_db_port", default="5432")
        api_key = Variable.get("staging_api_key", default="secret_staging_key")
    else:  # development
        db_host = Variable.get("dev_db_host", default="localhost")
        db_port = Variable.get("dev_db_port", default="5432")
        api_key = Variable.get("dev_api_key", default="dev_key")
    
    return {
        'env': env,
        'db_host': db_host,
        'db_port': db_port,
        'api_key': api_key
    }

@task
def get_processing_config(**context):
    """Lee configuración de procesamiento"""
    batch_size = int(Variable.get("batch_size", default="1000"))
    max_retries = int(Variable.get("max_retries", default="3"))
    enable_notifications = Variable.get("enable_notifications", default="true") == "true"
    
    print(f"Batch size: {batch_size}")
    print(f"Max retries: {max_retries}")
    print(f"Notifications: {enable_notifications}")
    
    return {
        'batch_size': batch_size,
        'max_retries': max_retries,
        'enable_notifications': enable_notifications
    }

@task
def process_data(env_config, proc_config, **context):
    """Procesa datos usando configuración de Variables"""
    print(f"Connecting to {env_config['db_host']}:{env_config['db_port']}")
    print(f"Using API key: {env_config['api_key'][:10]}...")
    print(f"Processing in batches of {proc_config['batch_size']}")
    
    records_processed = proc_config['batch_size'] * 5
    return {'records': records_processed}

@task
def notify_completion(result, proc_config, **context):
    """Notifica si está habilitado"""
    if proc_config['enable_notifications']:
        print(f"Sending notification: {result['records']} records processed")
    else:
        print("Notifications disabled, skipping")
    return {'notified': proc_config['enable_notifications']}

with DAG(
    dag_id='variables_challenge',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['challenge', 'variables'],
) as dag:
    
    env_config = get_environment_config()
    proc_config = get_processing_config()
    result = process_data(env_config, proc_config)
    notification = notify_completion(result, proc_config)
