"""
DESAFÍO: Pipeline de Reporte Financiero con Edge Labels

Crea un DAG que procese datos financieros con múltiples caminos
y usa edge labels para documentar completamente el flujo, las decisiones
y las transformaciones en cada paso.

REQUISITOS:

1. DAG Configuration:
   - dag_id: 'financial_reporting_pipeline'
   - schedule: '@daily'
   - Usar tags apropiados

2. Inicio y Preparación:
   - start: Tarea inicial
   - check_business_day: Verificar si es día hábil
   - Label desde start: 'Iniciar proceso'
   - Label desde check: 'Verificación completada'

3. Branching por Tipo de Día:
   - Usar BranchPythonOperator: decide_report_type
   - Si es fin de semana (días 5,6): 'weekend_summary'
   - Si es fin de mes (día 28-31): 'monthly_close'
   - Si es día normal: 'daily_reports'
   - Labels específicos para cada camino explicando la condición

4. Rama Weekend (fin de semana):
   - generate_weekly_summary
   - validate_week_totals
   - Labels: 'Datos semanales', 'Totales validados'

5. Rama Monthly (fin de mes):
   - aggregate_monthly_data
   - calculate_kpis
   - reconcile_accounts
   - generate_month_end_reports
   - Flujo secuencial con labels descriptivos

6. Rama Daily (día normal):
   - extract_transactions (paralelo con extract_balances)
   - extract_balances (paralelo con extract_transactions)
   - consolidate_daily_data
   - Labels: 'Transacciones', 'Balances', 'Consolidado'

7. Punto de Convergencia:
   - validate_all_reports (trigger_rule=ALL_DONE)
   - Labels desde cada rama explicando qué tipo de datos llegan

8. Procesamiento Final:
   - archive_reports
   - notify_stakeholders
   - update_dashboard
   - Las 3 tareas en paralelo
   - Labels: 'Archivados', 'Notificados', 'Dashboard actualizado'

9. Manejo de Errores:
   - log_errors (trigger_rule=ONE_FAILED)
   - send_alert
   - Labels: 'Error detectado', 'Alerta enviada'

10. Finalización:
    - cleanup (trigger_rule=ALL_DONE)
    - end
    - Labels: 'Limpieza completada', 'Proceso finalizado'

RESTRICCIONES:
- Usar ÚNICAMENTE: BashOperator, EmptyOperator, BranchPythonOperator
- CADA conexión entre tareas DEBE tener un Label descriptivo
- Mínimo 18 tareas en total
- Mínimo 20 Labels en total
- Los labels deben ser informativos, no genéricos ('ok', 'done', etc.)
- Demostrar branching, paralelo, trigger rules y convergencia

TIPS:
- Los labels pueden incluir emojis para mejor visualización
- Labels pueden indicar: tipo de datos, volumen, formato, condiciones
- Usa labels para documentar qué pasa en caso de éxito vs error
- La función de branching debe usar context['logical_date']
"""

import datetime

from airflow.sdk import DAG, Label, TriggerRule
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Define la función de branching

# TODO: Implementa el DAG según los requisitos
