"""
DESAFÍO: Sistema de Reportes Multi-Canal con Latest Only

Crea un DAG que procese datos de ventas con backfill habilitado,
pero que solo ejecute reportes y notificaciones en el run más reciente.

REQUISITOS:

1. DAG Configuration:
   - dag_id: 'sales_reporting_system'
   - schedule: '@daily'
   - Usar tags apropiados
   - IMPORTANTE: catchup=True (para demostrar el comportamiento de latest_only)

2. Fase de Extracción (SIEMPRE se ejecuta, importante para backfill):
   - Extraer datos de ventas del día
   - Extraer datos de inventario
   - Extraer datos de clientes
   - Estas 3 tareas deben ejecutarse en paralelo

3. Fase de Procesamiento (SIEMPRE se ejecuta):
   - Consolidar todos los datos
   - Calcular métricas de negocio
   - Validar calidad de datos

4. Checkpoint Latest Only:
   - Usar LatestOnlyOperator para separar procesamiento de reportes

5. Fase de Reportes (SOLO último run):
   - Generar reporte PDF
   - Generar reporte Excel
   - Actualizar dashboard Power BI
   - Publicar en sitio web

6. Fase de Notificaciones (SOLO último run):
   - Enviar email a ejecutivos
   - Enviar mensaje a Slack
   - Enviar notificación push a app móvil

7. Fase de Auditoría (SIEMPRE se ejecuta):
   - Registrar ejecución en base de auditoría
   - Archivar logs
   - Generar métricas de performance
   - Estas tareas deben usar trigger_rule=TriggerRule.ALL_DONE

8. Tarea Final (SIEMPRE se ejecuta):
   - Limpiar archivos temporales
   - Debe usar trigger_rule=TriggerRule.ALL_DONE

RESTRICCIONES:
- Usar ÚNICAMENTE: BashOperator, EmptyOperator, LatestOnlyOperator
- El DAG debe tener exactamente UN LatestOnlyOperator
- Mínimo 15 tareas en total
- Las tareas de extracción deben ser paralelas
- Las tareas de reportes deben ser paralelas
- Las tareas de notificaciones deben ser paralelas
- Las tareas de auditoría deben ejecutarse SIEMPRE (usar ALL_DONE)

ESTRUCTURA ESPERADA:
start -> [extractores paralelos] -> consolidar -> calcular -> validar 
  -> latest_only -> [reportes paralelos] -> [notificaciones paralelas]
  -> [auditoría paralela con ALL_DONE] -> cleanup con ALL_DONE

NOTA: Durante un backfill de 7 días:
- Las fases de extracción, procesamiento y auditoría se ejecutarán 7 veces
- Los reportes y notificaciones solo se ejecutarán en el 7mo día (último run)
"""

import datetime

from airflow.sdk import DAG, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

# TODO: Implementa el DAG según los requisitos
