"""
DESAFÍO: Data Warehouse ETL con Catchup Strategy

Crea un pipeline ETL que procesa datos históricos de forma inteligente,
usando la estrategia de catchup apropiada según el tipo de carga.

CONTEXTO:
Tienes un data warehouse que necesita 3 tipos de pipelines diferentes:
- Full refresh: Carga completa que reemplaza toda la tabla
- Incremental: Carga solo datos nuevos desde última ejecución
- SCD Type 2: Slowly Changing Dimensions con histórico

REQUISITOS:

1. DAG: Full Refresh Pipeline
   - dag_id: 'catchup_challenge_full_refresh'
   - schedule: '@weekly' (domingos a medianoche)
   - start_date: 30 días atrás
   - catchup: ¿True o False? (decide y justifica en doc_md)
   - Tags: incluir 'challenge', 'dag_runs', 'catchup', 'full_refresh'
   
   Pipeline (6 tareas mínimo):
   - start
   - truncate_target_table (limpia tabla destino)
   - extract_all_records (extrae TODOS los registros)
   - transform_full_dataset
   - load_complete_table
   - rebuild_indexes
   - end
   
   Doc_md debe explicar:
   - ¿Por qué elegiste catchup=True o False?
   - ¿Tiene sentido ejecutar full refresh históricos?
   - ¿Qué pasa si activas este DAG después de 1 año?

2. DAG: Incremental Pipeline
   - dag_id: 'catchup_challenge_incremental'
   - schedule: '@daily'
   - start_date: 7 días atrás
   - catchup: ¿True o False? (decide y justifica)
   - Tags: incluir 'challenge', 'dag_runs', 'catchup', 'incremental'
   
   Pipeline (7 tareas mínimo):
   - start
   - get_last_processed_date (determina desde cuándo cargar)
   - extract_new_records (WHERE date > last_date)
   - validate_no_duplicates
   - transform_incremental
   - load_append (agrega registros, no reemplaza)
   - update_watermark (guarda fecha procesada)
   - end
   
   Doc_md debe explicar:
   - ¿Por qué elegiste catchup=True o False?
   - ¿Cómo manejas datos faltantes si omitiste un día?
   - ¿Estrategia de recovery si falla un día?

3. DAG: SCD Type 2 Historical Pipeline
   - dag_id: 'catchup_challenge_scd_type2'
   - schedule: '@daily'
   - start_date: 14 días atrás
   - catchup: ¿True o False? (decide y justifica)
   - Tags: incluir 'challenge', 'dag_runs', 'catchup', 'scd'
   
   Pipeline (9 tareas mínimo):
   - start
   - extract_source_snapshot (snapshot completo del día)
   - extract_current_dimension (estado actual en warehouse)
   - identify_new_records
   - identify_changed_records
   - close_old_versions (set valid_to date)
   - insert_new_versions (set valid_from date)
   - insert_new_records
   - validate_history_integrity
   - end
   
   Doc_md debe explicar:
   - ¿Por qué elegiste catchup=True o False?
   - ¿Qué pasa si omites un día en SCD Type 2?
   - ¿Puedes reconstruir el histórico correcto con backfill?

RESTRICCIONES:
- Usar ÚNICAMENTE: BashOperator, EmptyOperator
- CADA DAG debe tener configuración de catchup explícita
- Los bash_command deben mostrar el uso de {{ ds }}, {{ logical_date }}
- Incluir doc_md explicando la decisión de catchup

DECISIONES TÉCNICAS A JUSTIFICAR:
Para CADA DAG, en el doc_md debes responder:
1. ¿Por qué elegiste catchup=True o False?
2. ¿Qué sucede si el DAG se desactiva por 1 mes y luego se reactiva?
3. ¿Tu pipeline puede procesar datos históricos correctamente?
4. ¿Prefieres catchup automático o backfill manual? ¿Por qué?

PUNTOS EXTRA:
- Usar {{ data_interval_start }} y {{ data_interval_end }} apropiadamente
- Mostrar validaciones de datos para cada tipo de carga
- Explicar estrategia de idempotencia
- Considerar impacto en scheduler y recursos

EJEMPLO DE DECISIÓN:
```python
# Full Refresh: probablemente catchup=False
# Razón: No tiene sentido ejecutar 30 full refreshes históricos,
# solo el más reciente importa. Los anteriores serían sobrescritos.

# Incremental: probablemente catchup=True
# Razón: Cada día tiene datos únicos que deben cargarse.
# Si omites un día, pierdes esos datos.
```

No implementes la lógica real de base de datos, solo simula con echo
mostrando los conceptos correctos.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Implementa los 3 DAGs según los requisitos
# Recuerda justificar cada decisión de catchup en el doc_md
