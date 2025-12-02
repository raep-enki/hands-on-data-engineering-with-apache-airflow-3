"""
Challenge: Un Pipeline de ML Que Nunca Se Rinde

Estás entrenando modelos de machine learning y el problema es que algunas fuentes de datos fallan.
Pero no puedes esperar a que TODAS las fuentes estén listas. Si al menos una fuente tiene datos,
quieres continuar. Entrenas tres versiones del modelo en paralelo (porque no sabes cuál será mejor).
Si al menos UNO de los modelos funciona, quieres desplegarlo. Al final, SIEMPRE limpias recursos.

Debes usar al menos 8 trigger_rules diferentes para manejar todos los escenarios de fallo y éxito.
Es un ejercicio avanzado de resiliencia donde demuestras que conoces todas las opciones disponibles.

**El flujo del pipeline resiliente:**

`start` (EmptyOperator) se ramifica a 4 fuentes de datos en paralelo:
- `fetch_source_a` (BashOperator - puede fallar 30% del tiempo)
- `fetch_source_b` (BashOperator - puede fallar 40% del tiempo)
- `fetch_source_c` (BashOperator - puede fallar 20% del tiempo)
- `fetch_source_d` (BashOperator - puede fallar 50% del tiempo, la más inestable)

**Primera convergencia** (con trigger_rule='one_success'):
`[todas las 4 fuentes]` >> `aggregate_available_data` (BashOperator - con `trigger_rule='one_success'`
porque basta que UNA fuente funcione para continuar. Consolida lo que esté disponible).

**Validación obligatoria** (con trigger_rule='all_success' - DEFAULT):
`aggregate_available_data` >> `validate_data_quality` (BashOperator - valida formato y completitud,
debe tener éxito obligatoriamente o el pipeline falla).

**Feature engineering** (con trigger_rule='none_failed'):
`validate_data_quality` >> `calculate_features` (BashOperator - con `trigger_rule='none_failed'`
porque si la validación fue skipped pero nada falló, igual continúa).

`calculate_features` se ramifica en 3 entrenamientos paralelos:
- `train_model_xgboost` (BashOperator - modelo XGBoost, puede fallar si no hay suficientes datos)
- `train_model_random_forest` (BashOperator - modelo Random Forest, más robusto)
- `train_model_neural_net` (BashOperator - modelo Neural Net, puede fallar por memoria)

**Evaluación de modelos** (cada modelo tiene su evaluación):
- `train_model_xgboost` >> `evaluate_xgboost` (BashOperator - calcula accuracy)
- `train_model_random_forest` >> `evaluate_random_forest` (BashOperator - calcula accuracy)
- `train_model_neural_net` >> `evaluate_neural_net` (BashOperator - calcula accuracy)

**Selección del mejor modelo** (con trigger_rule='one_success'):
`[evaluate_xgboost, evaluate_random_forest, evaluate_neural_net]` >> `select_best_model`
(BashOperator - con `trigger_rule='one_success'` porque basta que UN modelo haya funcionado.
Selecciona el mejor disponible entre los que completaron).

**Deployment condicional** (con trigger_rule='none_failed_min_one_success'):
`select_best_model` >> `deploy_to_staging` (BashOperator - con `trigger_rule='none_failed_min_one_success'`
porque solo deploya si algo upstream tuvo éxito Y nada falló crítico).

`deploy_to_staging` se ramifica en dos validaciones paralelas:
- `test_model_performance` (BashOperator - prueba latencia y throughput)
- `test_model_accuracy` (BashOperator - prueba contra test set)

**Deployment a producción** (con trigger_rule='all_success'):
`[test_model_performance, test_model_accuracy]` >> `deploy_to_production` (BashOperator -
con `trigger_rule='all_success'` DEFAULT porque AMBOS tests deben pasar para ir a prod).

**Path alternativo si falla deployment:**
`deploy_to_production` >> `notify_success` (BashOperator - envía Slack si deployment exitoso)
`deploy_to_staging` >> `rollback_to_previous` (BashOperator - con `trigger_rule='one_failed'`
se ejecuta SOLO si algún test falló, hace rollback al modelo anterior).

**Limpieza garantizada** (con trigger_rule='all_done'):
`[notify_success, rollback_to_previous]` >> `cleanup_temp_files` (BashOperator -
con `trigger_rule='all_done'` se ejecuta SIEMPRE sin importar qué pasó antes) >>

`cleanup_temp_files` >> `release_resources` (BashOperator - con `trigger_rule='all_done'`
libera GPU y memoria SIEMPRE) >> `end` (EmptyOperator con `trigger_rule='all_done'`).

**Trigger rules usados (8 diferentes):**
1. `all_success` (default) - validate_data_quality, deploy_to_production
2. `one_success` - aggregate_available_data, select_best_model
3. `none_failed` - calculate_features
4. `none_failed_min_one_success` - deploy_to_staging
5. `one_failed` - rollback_to_previous
6. `all_done` - cleanup_temp_files, release_resources, end
7. (implícito `all_success` en evaluaciones individuales)
8. (implícito `all_success` en tests paralelos)

**Configuración técnica:**
- DAG ID: `trigger_rules_challenge`
- Schedule: @daily
- Start date: 2024-01-01
- Catchup: False
- Tags: `['challenge', 'trigger_rules']`
- Al menos 20 tareas demostrando diferentes escenarios de fallo/éxito
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Solución del challenge

def branch_by_accuracy(**context):
    # Simulamos una decisión basada en accuracy
    accuracy = 0.88  # Simulación
    if accuracy >= 0.85:
        return 'deploy_model_to_staging'
    else:
        return 'notify_poor_performance'

with DAG(
    dag_id='trigger_rules_challenge',
    start_date=datetime.datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['challenge', 'trigger_rules'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # 4 fuentes de datos paralelas (pueden fallar)
    fetch_source_a = BashOperator(task_id='fetch_source_a', bash_command='echo "Fetching source A"')
    fetch_source_b = BashOperator(task_id='fetch_source_b', bash_command='echo "Fetching source B"')
    fetch_source_c = BashOperator(task_id='fetch_source_c', bash_command='echo "Fetching source C"')
    fetch_source_d = BashOperator(task_id='fetch_source_d', bash_command='echo "Fetching source D"')
    
    # Agregación con trigger_rule='one_success'
    aggregate_available_data = BashOperator(
        task_id='aggregate_available_data',
        bash_command='echo "Aggregating available data"',
        trigger_rule='one_success',
    )
    
    # Validación con trigger_rule='all_success' (default)
    validate_data_quality = BashOperator(
        task_id='validate_data_quality',
        bash_command='echo "Validating data quality"',
    )
    
    # Feature engineering con trigger_rule='none_failed'
    calculate_features = BashOperator(
        task_id='calculate_features',
        bash_command='echo "Calculating features"',
        trigger_rule='none_failed',
    )
    
    # Entrenamiento de 3 modelos en paralelo
    train_model_xgboost = BashOperator(
        task_id='train_model_xgboost',
        bash_command='echo "Training XGBoost model"',
    )
    train_model_random_forest = BashOperator(
        task_id='train_model_random_forest',
        bash_command='echo "Training Random Forest model"',
    )
    train_model_neural_net = BashOperator(
        task_id='train_model_neural_net',
        bash_command='echo "Training Neural Net model"',
    )
    
    # Evaluación de modelos
    evaluate_xgboost = BashOperator(
        task_id='evaluate_xgboost',
        bash_command='echo "Evaluating XGBoost: accuracy 0.92"',
    )
    evaluate_random_forest = BashOperator(
        task_id='evaluate_random_forest',
        bash_command='echo "Evaluating Random Forest: accuracy 0.89"',
    )
    evaluate_neural_net = BashOperator(
        task_id='evaluate_neural_net',
        bash_command='echo "Evaluating Neural Net: accuracy 0.94"',
    )
    
    # Selección del mejor modelo con trigger_rule='one_success'
    select_best_model = BashOperator(
        task_id='select_best_model',
        bash_command='echo "Selecting best model among available ones"',
        trigger_rule='one_success',
    )
    
    # Deployment a staging con trigger_rule='none_failed_min_one_success'
    deploy_to_staging = BashOperator(
        task_id='deploy_to_staging',
        bash_command='echo "Deploying to staging"',
        trigger_rule='none_failed_min_one_success',
    )
    
    # Tests en paralelo
    test_model_performance = BashOperator(
        task_id='test_model_performance',
        bash_command='echo "Testing model performance"',
    )
    test_model_accuracy = BashOperator(
        task_id='test_model_accuracy',
        bash_command='echo "Testing model accuracy"',
    )
    
    # Deployment a producción con trigger_rule='all_success' (default)
    deploy_to_production = BashOperator(
        task_id='deploy_to_production',
        bash_command='echo "Deploying to production"',
    )
    
    # Notificación de éxito
    notify_success = BashOperator(
        task_id='notify_success',
        bash_command='echo "Sending success notification to Slack"',
    )
    
    # Rollback con trigger_rule='one_failed'
    rollback_to_previous = BashOperator(
        task_id='rollback_to_previous',
        bash_command='echo "Rolling back to previous model"',
        trigger_rule='one_failed',
    )
    
    # Limpieza garantizada con trigger_rule='all_done'
    cleanup_temp_files = BashOperator(
        task_id='cleanup_temp_files',
        bash_command='echo "Cleaning up temp files"',
        trigger_rule='all_done',
    )
    
    release_resources = BashOperator(
        task_id='release_resources',
        bash_command='echo "Releasing GPU and memory"',
        trigger_rule='all_done',
    )
    
    end = EmptyOperator(task_id='end', trigger_rule='all_done')
    
    # Dependencies
    start >> [fetch_source_a, fetch_source_b, fetch_source_c, fetch_source_d]
    [fetch_source_a, fetch_source_b, fetch_source_c, fetch_source_d] >> aggregate_available_data
    aggregate_available_data >> validate_data_quality >> calculate_features
    
    calculate_features >> [train_model_xgboost, train_model_random_forest, train_model_neural_net]
    
    train_model_xgboost >> evaluate_xgboost
    train_model_random_forest >> evaluate_random_forest
    train_model_neural_net >> evaluate_neural_net
    
    [evaluate_xgboost, evaluate_random_forest, evaluate_neural_net] >> select_best_model
    select_best_model >> deploy_to_staging
    
    deploy_to_staging >> [test_model_performance, test_model_accuracy]
    [test_model_performance, test_model_accuracy] >> deploy_to_production
    
    deploy_to_production >> notify_success
    [test_model_performance, test_model_accuracy] >> rollback_to_previous
    
    [notify_success, rollback_to_previous] >> cleanup_temp_files
    cleanup_temp_files >> release_resources >> end
