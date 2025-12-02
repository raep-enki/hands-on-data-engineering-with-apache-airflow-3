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

# Solución del challenge - DAG 1: Procesar pedidos en tiempo real
with DAG(
    dag_id='delivery_realtime_orders',
    start_date=datetime.datetime(2024, 11, 1),
    schedule='*/1 * * * *',
    catchup=False,
    tags=['challenge', 'running_dags'],
) as dag1:
    inicio = EmptyOperator(task_id='inicio')
    recibir_pedido = BashOperator(task_id='recibir_pedido', bash_command='echo "Pedido recibido"')
    validar_inventario = BashOperator(task_id='validar_inventario', bash_command='echo "Inventario OK"')
    asignar_rider = BashOperator(task_id='asignar_rider', bash_command='echo "Rider asignado"')
    enviar_notificacion_restaurante = BashOperator(task_id='enviar_notificacion_restaurante', bash_command='echo "Notificación enviada"')
    fin = EmptyOperator(task_id='fin')
    
    inicio >> recibir_pedido >> validar_inventario >> asignar_rider >> enviar_notificacion_restaurante >> fin

# DAG 2: Sincronizar inventarios
with DAG(
    dag_id='delivery_inventory_sync',
    start_date=datetime.datetime(2024, 11, 1),
    schedule='*/5 * * * *',
    catchup=False,
    tags=['challenge', 'running_dags'],
) as dag2:
    inicio = EmptyOperator(task_id='inicio')
    consultar_inventario = BashOperator(task_id='consultar_inventario', bash_command='echo "Consultando inventarios"')
    actualizar_disponibilidad = BashOperator(task_id='actualizar_disponibilidad', bash_command='echo "Disponibilidad actualizada"')
    notificar_cambios = BashOperator(task_id='notificar_cambios', bash_command='echo "Cambios notificados"')
    fin = EmptyOperator(task_id='fin')
    
    inicio >> consultar_inventario >> actualizar_disponibilidad >> notificar_cambios >> fin

# DAG 3: Actualizar ubicaciones de riders
with DAG(
    dag_id='delivery_rider_location',
    start_date=datetime.datetime(2024, 11, 1),
    schedule='*/2 * * * *',
    catchup=False,
    tags=['challenge', 'running_dags'],
) as dag3:
    inicio = EmptyOperator(task_id='inicio')
    obtener_ubicaciones = BashOperator(task_id='obtener_ubicaciones', bash_command='echo "Ubicaciones obtenidas"')
    actualizar_mapa = BashOperator(task_id='actualizar_mapa', bash_command='echo "Mapa actualizado"')
    calcular_tiempos_entrega = BashOperator(task_id='calcular_tiempos_entrega', bash_command='echo "ETAs calculados"')
    fin = EmptyOperator(task_id='fin')
    
    inicio >> obtener_ubicaciones >> actualizar_mapa >> calcular_tiempos_entrega >> fin

# DAG 4: Monitor de estado de órdenes
with DAG(
    dag_id='delivery_order_status',
    start_date=datetime.datetime(2024, 11, 1),
    schedule='*/1 * * * *',
    catchup=False,
    tags=['challenge', 'running_dags'],
) as dag4:
    inicio = EmptyOperator(task_id='inicio')
    check_pending = BashOperator(task_id='check_pending', bash_command='echo "Pending orders: 5"')
    check_en_camino = BashOperator(task_id='check_en_camino', bash_command='echo "In delivery: 12"')
    check_retrasados = BashOperator(task_id='check_retrasados', bash_command='echo "Delayed: 2"')
    actualizar_dashboard = BashOperator(task_id='actualizar_dashboard', bash_command='echo "Dashboard updated"')
    fin = EmptyOperator(task_id='fin')
    
    inicio >> [check_pending, check_en_camino, check_retrasados] >> actualizar_dashboard >> fin

# DAG 5: Reporte de cierre diario
with DAG(
    dag_id='delivery_cierre_diario',
    start_date=datetime.datetime(2024, 11, 1),
    schedule='30 23 * * *',
    catchup=False,
    tags=['challenge', 'running_dags'],
) as dag5:
    inicio = EmptyOperator(task_id='inicio')
    consolidar_pedidos_dia = BashOperator(task_id='consolidar_pedidos_dia', bash_command='echo "Consolidando pedidos del día"')
    revenue_total = BashOperator(task_id='revenue_total', bash_command='echo "Revenue: $50,000"')
    horas_pico = BashOperator(task_id='horas_pico', bash_command='echo "Peak: 12-2pm, 7-9pm"')
    performance_riders = BashOperator(task_id='performance_riders', bash_command='echo "Avg rating: 4.7"')
    cancelaciones = BashOperator(task_id='cancelaciones', bash_command='echo "Cancellations: 2.3%"')
    generar_reporte_gerencial = BashOperator(task_id='generar_reporte_gerencial', bash_command='echo "Reporte generado"')
    enviar_notificaciones = BashOperator(task_id='enviar_notificaciones', bash_command='echo "Notificaciones enviadas"')
    fin = EmptyOperator(task_id='fin')
    
    inicio >> consolidar_pedidos_dia
    consolidar_pedidos_dia >> [revenue_total, horas_pico, performance_riders, cancelaciones]
    [revenue_total, horas_pico, performance_riders, cancelaciones] >> generar_reporte_gerencial
    generar_reporte_gerencial >> enviar_notificaciones >> fin
