"""
DESAFÍO: Pipeline de Data Science con Task Groups

Crea un DAG que implemente un pipeline completo de Data Science
organizado con task groups para mejorar la visualización y mantenibilidad.

REQUISITOS:

1. DAG Configuration:
   - dag_id: 'ml_pipeline_with_task_groups'
   - schedule: '@weekly'
   - Usar tags apropiados

2. Grupo: Data Collection (group_id='data_collection')
   - Extraer datos de entrenamiento (3 fuentes en paralelo):
     * from_production_db
     * from_feature_store
     * from_external_api
   - Tooltip: 'Recolección de datos desde múltiples fuentes'

3. Grupo: Data Preparation (group_id='data_prep')
   - Limpiar datos (clean_data)
   - Manejar valores faltantes (handle_missing_values)
   - Codificar variables categóricas (encode_categorical)
   - Dividir train/test (split_train_test)
   - Flujo secuencial dentro del grupo
   - Tooltip: 'Preparación y limpieza de datos'

4. Grupo: Feature Engineering (group_id='feature_engineering')
   - Crear features numéricas (create_numeric_features)
   - Crear features categóricas (create_categorical_features)
   - Crear features de interacción (create_interaction_features)
   - Las 3 tareas en paralelo
   - Consolidar features (consolidate_features)
   - Tooltip: 'Ingeniería de características'

5. Grupo Anidado: Model Training (group_id='model_training')
   - Subgrupo: Hyperparameter Tuning (group_id='hyperparameter_tuning')
     * Probar configuración 1, 2 y 3 en paralelo (config_1, config_2, config_3)
     * Seleccionar mejor configuración (select_best_config)
   - Entrenar modelo final (train_final_model)
   - Tooltip: 'Entrenamiento y optimización del modelo'

6. Grupo: Model Evaluation (group_id='model_evaluation')
   - Evaluar en train set (evaluate_on_train)
   - Evaluar en test set (evaluate_on_test)
   - Evaluar en validation set (evaluate_on_validation)
   - Las 3 evaluaciones en paralelo
   - Generar reporte de métricas (generate_metrics_report)
   - Tooltip: 'Evaluación del modelo'

7. Grupo: Model Deployment (group_id='deployment')
   - Validar modelo para producción (validate_for_production)
   - Registrar modelo (register_model)
   - Desplegar a staging (deploy_to_staging)
   - Ejecutar smoke tests (run_smoke_tests)
   - Flujo secuencial
   - Tooltip: 'Despliegue del modelo a producción'

8. Tareas Fuera de Grupos:
   - start: Tarea inicial
   - notify_completion: Notificar finalización (después de deployment)
   - end: Tarea final

ESTRUCTURA ESPERADA:
- 6 task groups (1 anidado)
- Mínimo 20 tareas en total
- Usar tooltips en todos los grupos
- Demostrar dependencias cruzadas entre grupos
- Al menos un grupo con tareas en paralelo
- Al menos un grupo con tareas secuenciales

RESTRICCIONES:
- Usar ÚNICAMENTE: BashOperator, EmptyOperator, TaskGroup
- NO usar @task_group decorator (usar TaskGroup como context manager)
- Los task_ids dentro de grupos deben ser descriptivos
- Flujo principal: data_collection >> data_prep >> feature_engineering >> 
  model_training >> model_evaluation >> deployment

TIPS:
- Usa 'with TaskGroup(group_id=..., tooltip=...) as variable:'
- Para dependencias cruzadas: group.get_task('task_id')
- Los comandos bash pueden ser simples echo statements
"""

import datetime

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa el DAG según los requisitos
