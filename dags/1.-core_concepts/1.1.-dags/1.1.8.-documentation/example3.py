"""
# ML Model Training Pipeline

## Overview
Pipeline automatizado para reentrenamiento mensual del modelo de predicción
de churn de clientes.

## Business Context
El modelo de churn permite al equipo de retención identificar clientes
en riesgo y tomar acciones preventivas. Se entrena mensualmente con
datos históricos de los últimos 12 meses.

## Technical Stack
- **ML Framework**: Scikit-learn 1.3.0
- **Feature Store**: Feast
- **Model Registry**: MLflow
- **Compute**: AWS EC2 (r5.4xlarge)

## Monitoring
- **Drift Detection**: Evidently AI
- **Performance Metrics**: Stored in `ml_metrics.model_performance`
- **Alertas**: Slack channel #ml-alerts

## SLA
- Inicio: Primer día del mes a las 00:00 UTC
- Duración esperada: 2-3 horas
- Deadline: Antes de las 06:00 UTC
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='documentation_with_context',
    schedule='0 0 1 * *',  # Primer día de cada mes
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'core_concepts', 'dags', 'documentation', 'ml']
) as dag:
    dag.doc_md = __doc__
    
    start = EmptyOperator(task_id='start')
    
    extract_features = BashOperator(
        task_id='extract_features',
        bash_command='echo "📥 Extrayendo features"'
    )
    extract_features.doc_md = """
    ### Extract Features from Feature Store
    
    Obtiene las features preparadas desde Feast Feature Store.
    
    #### Feature Groups
    - **Customer Demographics** (12 features)
      - age_group, location, account_age, etc.
    
    - **Behavioral Features** (25 features)
      - login_frequency, avg_session_duration, feature_usage, etc.
    
    - **Transaction Features** (18 features)
      - total_spend, avg_transaction, purchase_frequency, etc.
    
    - **Support Features** (8 features)
      - ticket_count, avg_resolution_time, satisfaction_score, etc.
    
    **Total Features**: 63  
    **Historical Window**: 12 meses  
    **Point-in-time correct**: Sí
    
    #### Output
    - Formato: Parquet
    - Particiones: Por mes
    - Ubicación: `s3://ml-data/features/churn/{{ ds }}/`
    """
    
    train = BashOperator(
        task_id='train_model',
        bash_command='echo "🤖 Entrenando modelo"'
    )
    train.doc_md = """
    ### Train Churn Prediction Model
    
    Entrena un modelo de clasificación binaria (churn vs no-churn).
    
    #### Algorithm
    **Primary**: XGBoost Classifier
    - Max depth: 6
    - Learning rate: 0.1
    - N estimators: 100
    - Early stopping: 10 rounds
    
    #### Hyperparameter Tuning
    - Method: Bayesian Optimization (Optuna)
    - Trials: 50
    - CV: 5-fold stratified
    - Metric: ROC-AUC
    
    #### Training Data
    - Positives (churn): ~15% de la muestra
    - Negatives (no churn): ~85%
    - Balancing: SMOTE (Synthetic Minority Oversampling)
    - Train/Val/Test split: 70/15/15
    
    **Tiempo estimado**: 45-60 minutos
    """
    
    evaluate = BashOperator(
        task_id='evaluate_model',
        bash_command='echo "📊 Evaluando modelo"'
    )
    evaluate.doc_md = """
    ### Evaluate Model Performance
    
    Evalúa el modelo en el test set y compara con el modelo en producción.
    
    #### Métricas Calculadas
    - **ROC-AUC**: Objetivo > 0.85
    - **Precision@K**: K = top 10% más probable churn
    - **Recall@K**: Cobertura de churners reales
    - **F1-Score**: Balance precision-recall
    - **Calibration**: Brier score, calibration curve
    
    #### Comparación con Producción
    - Si nuevo modelo > producción + 2%: Promover
    - Si nuevo modelo < producción - 5%: Rechazar + alerta
    - Si diferencia < 2%: Revisar manualmente
    
    #### Drift Detection
    - Feature drift (PSI > 0.25): Alerta
    - Label drift (cambio en tasa de churn > 10%): Alerta
    - Prediction drift (KS test p-value < 0.05): Alerta
    """
    
    register = BashOperator(
        task_id='register_in_mlflow',
        bash_command='echo "📝 Registrando modelo"'
    )
    register.doc_md = """
    ### Register Model in MLflow
    
    Registra el modelo entrenado en MLflow Model Registry.
    
    #### Artifacts Guardados
    - Modelo serializado (pickle)
    - Feature importance plot
    - Confusion matrix
    - ROC curve
    - Calibration plot
    - SHAP values (sample)
    
    #### Metadata
    - Training date
    - Features used (names + versions)
    - Hyperparameters
    - Métricas de evaluación
    - Dataset statistics
    - Git commit hash
    
    #### Tags
    - model_type: xgboost_classifier
    - use_case: churn_prediction
    - training_month: {{ ds }}
    """
    
    deploy = BashOperator(
        task_id='deploy_to_staging',
        bash_command='echo "🚀 Desplegando a staging"'
    )
    deploy.doc_md = """
    ### Deploy to Staging Environment
    
    Despliega el nuevo modelo al ambiente de staging para validación.
    
    #### Endpoint de Staging
    - URL: `https://ml-staging.company.com/churn/predict`
    - Método: POST
    - Auth: API Key
    - Timeout: 30s
    
    #### Validaciones en Staging
    1. **Smoke Tests**
       - 100 predicciones de casos conocidos
       - Validar formato de respuesta
       - Validar tiempos de latencia < 100ms
    
    2. **Shadow Mode**
       - Ejecutar en paralelo con producción por 24h
       - Comparar predicciones
       - Analizar discrepancias
    
    3. **A/B Test** (si pasa shadow mode)
       - 10% de tráfico al nuevo modelo
       - Monitorear por 3 días
       - Comparar métricas de negocio
    
    **Rollout a Producción**: Manual después de aprobación
    """
    
    end = EmptyOperator(task_id='end')
    
    start >> extract_features >> train >> evaluate >> register >> deploy >> end
