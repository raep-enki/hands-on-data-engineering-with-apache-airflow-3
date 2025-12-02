"""
Challenge: Una Startup de Delivery Que Crece Rápido

Acabas de unirte a una startup de delivery de comida que está creciendo exponencialmente. Tienen
datos por todos lados pero ninguna orquestación clara. Te piden crear varios pipelines con diferentes
frecuencias según la necesidad del negocio.

Los pedidos llegan en tiempo real (cada minuto hay que procesarlos). El inventario de restaurantes
se sincroniza cada 5 minutos. La ubicación de los riders se actualiza cada 2 minutos. Hay un monitor
de estado de órdenes que corre cada minuto. Y al final del día (23:30) necesitan un reporte completo
de cierre con todas las métricas del día.

**DAG 1: Procesar Pedidos en Tiempo Real**
- DAG ID: `delivery_realtime_orders`
- Schedule: `*/1 * * * *` (cada minuto)
- Tareas: `inicio` → `recibir_pedido` (lee cola de pedidos) → `validar_inventario` (verifica disponibilidad)
  → `asignar_rider` (encuentra rider más cercano) → `enviar_notificacion_restaurante` → `fin`

**DAG 2: Sincronizar Inventarios**
- DAG ID: `delivery_inventory_sync`
- Schedule: `*/5 * * * *` (cada 5 minutos)
- Tareas: `inicio` → `consultar_inventario` (pregunta a cada restaurante) → `actualizar_disponibilidad` 
  (actualiza BD central) → `notificar_cambios` (avisa si algo se agotó) → `fin`

**DAG 3: Actualizar Ubicaciones de Riders**
- DAG ID: `delivery_rider_location`
- Schedule: `*/2 * * * *` (cada 2 minutos)
- Tareas: `inicio` → `obtener_ubicaciones` (GPS de cada rider) → `actualizar_mapa` (actualiza mapa en vivo)
  → `calcular_tiempos_entrega` (estima ETAs) → `fin`

**DAG 4: Monitor de Estado de Órdenes**
- DAG ID: `delivery_order_status`
- Schedule: `*/1 * * * *` (cada minuto)
- Tareas: `inicio` → [en paralelo: `check_pending` (órdenes esperando), `check_en_camino` (en delivery),
  `check_retrasados` (con retraso)] → `actualizar_dashboard` (dashboard en vivo) → `fin`

**DAG 5: Reporte de Cierre Diario**
- DAG ID: `delivery_cierre_diario`
- Schedule: `30 23 * * *` (23:30 todos los días)
- Tareas: `inicio` → `consolidar_pedidos_dia` (cuenta todo el día) → [en paralelo: `revenue_total`,
  `horas_pico`, `performance_riders`, `cancelaciones`] → `generar_reporte_gerencial` → 
  `enviar_notificaciones` (a gerentes) → `fin`

**Configuración técnica:**
- Todos desde: 2024-11-01
- Catchup: False
- Tags: `['challenge', 'running_dags']`
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# TODO: Crea los 5 DAGs con sus schedules específicos
