"""
Challenge: Pasar Objetos Complejos Entre Tareas

En el challenge anterior pasabas números simples o strings con XComs. Pero ¿qué pasa cuando necesitas
pasar estructuras complejas? Configuraciones de ML con 20 hiperparámetros, estadísticas de features,
artefactos de modelos con metadata, reportes con matrices y métricas complejas.

Python tiene **dataclasses** perfectas para esto: defines la estructura de tus datos como clases
tipadas, y TaskFlow las serializa/deserializa automáticamente. Es como pasar objetos entre funciones,
pero las funciones están en tareas distribuidas.

**NOTA**: Airflow 3.x tiene un bug conocido con dataclasses en template rendering. Como workaround,
usaremos dicts tipados con TypedDict hasta que se resuelva el issue.

**IMPORTANTE: Todas las tareas en este challenge usan PythonOperator (@task)** porque involucran:
- Procesamiento de objetos Python complejos
- Validaciones y transformaciones de datos
- Lógica de machine learning
- Decisiones basadas en métricas

No uses BashOperator para lógica compleja. Reserva bash para comandos shell simples.

**El pipeline de ML completo (config → data → train → evaluate → deploy):**

Crea el DAG `passing_objects_challenge` con:
- Diccionarios estructurados para: ModelConfig, DatasetStats, TrainedModel, EvaluationMetrics
- 5 tareas @task que pasen estos objetos entre sí
- Reporte consolidando TODOS los objetos

**Configuración técnica:**
- DAG ID: `passing_objects_challenge`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'passing_arbitrary_objects_as_arguments']`
- Usa PythonOperator (@task) para TODAS las tareas (no BashOperator)
"""

import datetime

from airflow.sdk import DAG, task

# Solución del challenge

@task
def create_model_config() -> dict:
    """Crea configuración del modelo como dict"""
    return {
        'learning_rate': 0.01,
        'max_depth': 10,
        'n_estimators': 100,
        'min_samples_split': 5
    }

@task
def analyze_dataset() -> dict:
    """Analiza dataset y retorna estadísticas"""
    return {
        'total_rows': 10000,
        'features': 25,
        'missing_values': 150,
        'class_balance': {'class_0': 7000, 'class_1': 3000}
    }

@task
def train_model(config: dict, stats: dict) -> dict:
    """Entrena modelo con config y stats"""
    print(f"Training with lr={config['learning_rate']}, depth={config['max_depth']}")
    print(f"Dataset: {stats['total_rows']} rows, {stats['features']} features")
    
    return {
        'model_id': 'xgb_v1',
        'config': config,
        'training_time': 125.5,
        'model_size_mb': 45.2
    }

@task
def evaluate_model(model: dict) -> dict:
    """Evalúa el modelo entrenado"""
    print(f"Evaluating {model['model_id']}")
    
    return {
        'accuracy': 0.92,
        'precision': 0.89,
        'recall': 0.94,
        'f1_score': 0.915
    }

@task
def generate_report(config: dict, stats: dict, model: dict, metrics: dict) -> dict:
    """Genera reporte consolidado"""
    print("=== ML TRAINING REPORT ===")
    print(f"Model: {model['model_id']}")
    print(f"Config: lr={config['learning_rate']}, estimators={config['n_estimators']}")
    print(f"Dataset: {stats['total_rows']} rows")
    print(f"Metrics: accuracy={metrics['accuracy']:.2%}, f1={metrics['f1_score']:.3f}")
    
    return {
        'status': 'completed',
        'model_id': model['model_id'],
        'accuracy': metrics['accuracy']
    }

with DAG(
    dag_id='passing_objects_challenge',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['challenge', 'passing_arbitrary_objects_as_arguments'],
) as dag:
    
    config = create_model_config()
    stats = analyze_dataset()
    model = train_model(config, stats)
    metrics = evaluate_model(model)
    report = generate_report(config, stats, model, metrics)

