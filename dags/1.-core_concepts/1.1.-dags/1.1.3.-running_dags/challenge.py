"""
# DESAFÍO: Sistema de Monitoreo y Reportes Multi-Frecuencia

## Objetivo
Crear un sistema completo de monitoreo y reportes para una plataforma de entrega de comida,
con DAGs que se ejecutan en diferentes frecuencias según las necesidades del negocio.

## Contexto del Negocio
Eres el Data Engineer de una plataforma de entrega de comida que opera 24/7.
Necesitas crear DAGs para diferentes procesos de monitoreo y reportes que se ejecutan
a distintas frecuencias.

## Requisitos

### DAG 1: Monitoreo de Pedidos en Tiempo Cercano-Real
- **DAG ID**: `delivery_monitoreo_pedidos_activos`
- **Schedule**: Cada 3 minutos (`*/3 * * * *`)
- **Start Date**: Hoy
- **Catchup**: False
- **Description**: Monitorea pedidos activos y tiempos de entrega
- **Tags**: `['challenge', 'core_concepts', 'dags', 'running_dags', 'delivery', 'monitoring']`

**Tareas (usar BashOperator con echo):**
1. `inicio` (EmptyOperator)
2. `consultar_pedidos_activos` - "Consultando pedidos en preparación y en camino"
3. `calcular_tiempos_entrega` - "Calculando tiempo promedio de entrega actual"
4. `detectar_retrasos` - "Detectando pedidos con retraso > 45 minutos"
5. `alertar_si_necesario` - "Generando alertas para pedidos retrasados"
6. `fin` (EmptyOperator)

**Dependencias**: inicio >> consultar >> calcular >> detectar >> alertar >> fin

---

### DAG 2: Actualización de Dashboard Operacional
- **DAG ID**: `delivery_dashboard_operacional`
- **Schedule**: Cada 10 minutos (`*/10 * * * *`)
- **Start Date**: Hoy
- **Catchup**: False
- **Description**: Actualiza métricas del dashboard operacional
- **Tags**: `['challenge', 'core_concepts', 'dags', 'running_dags', 'delivery', 'dashboard']`

**Tareas:**
1. `inicio` (EmptyOperator)
2. `calcular_pedidos_hora` - "Calculando pedidos de la última hora"
3. `calcular_revenue_hora` - "Calculando revenue de la última hora"
4. `calcular_riders_activos` - "Contando riders activos en este momento"
5. `calcular_restaurantes_activos` - "Contando restaurantes con pedidos activos"
6. `actualizar_metricas_dashboard` - "Actualizando dashboard en tiempo real"
7. `fin` (EmptyOperator)

**Dependencias**: inicio >> [pedidos, revenue, riders, restaurantes] >> actualizar >> fin

---

### DAG 3: Reporte Horario de Operaciones
- **DAG ID**: `delivery_reporte_horario`
- **Schedule**: Cada hora en el minuto 5 (`5 * * * *`)
- **Start Date**: 2024-11-25
- **Catchup**: False
- **Description**: Genera reporte detallado de la hora anterior
- **Tags**: `['challenge', 'core_concepts', 'dags', 'running_dags', 'delivery', 'hourly']`

**Tareas:**
1. `inicio` (EmptyOperator)
2. `agregar_pedidos_hora` - "Agregando pedidos de la última hora"
3. `calcular_tiempo_promedio_entrega` - "Calculando tiempo promedio de entrega"
4. `calcular_satisfaccion_cliente` - "Calculando rating promedio de clientes"
5. `identificar_top_restaurantes` - "Identificando top 10 restaurantes de la hora"
6. `identificar_zonas_demanda` - "Identificando zonas con mayor demanda"
7. `generar_reporte` - "Generando reporte horario completo"
8. `fin` (EmptyOperator)

**Dependencias**: inicio >> agregar >> [tiempo, satisfaccion, restaurantes, zonas] >> generar >> fin

---

### DAG 4: Sincronización con Restaurantes (Horario Laboral)
- **DAG ID**: `delivery_sync_restaurantes`
- **Schedule**: Cada 2 horas de 8 AM a 10 PM (`0 8-22/2 * * *`)
- **Start Date**: 2024-11-01
- **Catchup**: False
- **Description**: Sincroniza menús y disponibilidad con restaurantes
- **Tags**: `['challenge', 'core_concepts', 'dags', 'running_dags', 'delivery', 'sync']`

**Tareas:**
1. `inicio` (EmptyOperator)
2. `consultar_restaurantes_activos` - "Consultando lista de restaurantes activos"
3. `sincronizar_menus` - "Sincronizando menús y precios"
4. `sincronizar_disponibilidad` - "Sincronizando disponibilidad de productos"
5. `actualizar_tiempos_preparacion` - "Actualizando tiempos estimados de preparación"
6. `verificar_inconsistencias` - "Verificando inconsistencias en catálogo"
7. `fin` (EmptyOperator)

**Dependencias**: inicio >> consultar >> [menus, disponibilidad, tiempos] >> verificar >> fin

---

### DAG 5: Reporte de Cierre Diario
- **DAG ID**: `delivery_cierre_diario`
- **Schedule**: Diario a las 11:30 PM (`30 23 * * *`)
- **Start Date**: 2024-11-01
- **Catchup**: False
- **Description**: Genera reporte completo del día
- **Tags**: `['challenge', 'core_concepts', 'dags', 'running_dags', 'delivery', 'daily']`

**Tareas:**
1. `inicio` (EmptyOperator)
2. `consolidar_pedidos_dia` - "Consolidando todos los pedidos del día"
3. `calcular_revenue_total` - "Calculando revenue total del día"
4. `analizar_horas_pico` - "Analizando horas pico de demanda"
5. `evaluar_performance_riders` - "Evaluando performance de riders"
6. `calcular_cancelaciones` - "Calculando tasa de cancelación"
7. `generar_reporte_gerencial` - "Generando reporte para gerencia"
8. `enviar_notificaciones` - "Enviando reporte a stakeholders"
9. `fin` (EmptyOperator)

**Dependencias**: 
- inicio >> consolidar
- consolidar >> [revenue, horas_pico, performance, cancelaciones]
- [revenue, horas_pico, performance, cancelaciones] >> generar >> enviar >> fin

---

### DAG 6: Reporte Semanal (Domingos)
- **DAG ID**: `delivery_reporte_semanal`
- **Schedule**: Domingos a las 10 PM (`0 22 * * 0`)
- **Start Date**: 2024-11-01
- **Catchup**: False
- **Description**: Análisis semanal de tendencias y performance
- **Tags**: `['challenge', 'core_concepts', 'dags', 'running_dags', 'delivery', 'weekly']`

**Tareas:**
1. `inicio` (EmptyOperator)
2. `agregar_semana` - "Agregando datos de toda la semana"
3. `comparar_semana_anterior` - "Comparando con semana anterior (WoW)"
4. `analizar_tendencias_pedidos` - "Analizando tendencias de pedidos"
5. `evaluar_nuevos_restaurantes` - "Evaluando performance de restaurantes nuevos"
6. `identificar_oportunidades` - "Identificando oportunidades de mejora"
7. `generar_reporte_ejecutivo` - "Generando reporte ejecutivo semanal"
8. `fin` (EmptyOperator)

**Dependencias**: inicio >> agregar >> comparar >> [tendencias, restaurantes, oportunidades] >> generar >> fin

---

### DAG 7: Limpieza de Logs (Mensual)
- **DAG ID**: `delivery_limpieza_logs`
- **Schedule**: Día 1 de cada mes a las 4 AM (`0 4 1 * *`)
- **Start Date**: 2024-11-01
- **Catchup**: False
- **Description**: Limpieza y archivo de logs antiguos
- **Tags**: `['challenge', 'core_concepts', 'dags', 'running_dags', 'delivery', 'maintenance']`

**Tareas:**
1. `inicio` (EmptyOperator)
2. `identificar_logs_antiguos` - "Identificando logs de más de 90 días"
3. `comprimir_logs` - "Comprimiendo logs para archivo"
4. `mover_a_storage_frio` - "Moviendo logs a storage de largo plazo"
5. `eliminar_logs_locales` - "Eliminando logs de base de datos activa"
6. `optimizar_tablas` - "Optimizando tablas de logs"
7. `fin` (EmptyOperator)

**Dependencias**: inicio >> identificar >> comprimir >> mover >> eliminar >> optimizar >> fin

---

### DAG 8: Recalculo Manual de Métricas
- **DAG ID**: `delivery_recalculo_metricas`
- **Schedule**: None (solo trigger manual)
- **Start Date**: 2024-11-01
- **Catchup**: False
- **Description**: Recalcula métricas históricas bajo demanda
- **Tags**: `['challenge', 'core_concepts', 'dags', 'running_dags', 'delivery', 'manual']`

**Tareas:**
1. `inicio` (EmptyOperator)
2. `validar_rango_fechas` - "Validando rango de fechas para recalculo"
3. `cargar_datos_historicos` - "Cargando datos históricos del período"
4. `recalcular_pedidos` - "Recalculando métricas de pedidos"
5. `recalcular_revenue` - "Recalculando métricas de revenue"
6. `recalcular_satisfaccion` - "Recalculando métricas de satisfacción"
7. `actualizar_base_datos` - "Actualizando métricas en base de datos"
8. `generar_reporte_cambios` - "Generando reporte de cambios realizados"
9. `fin` (EmptyOperator)

**Dependencias**: inicio >> validar >> cargar >> [pedidos, revenue, satisfaccion] >> actualizar >> reporte >> fin

---

## Restricciones

1. **Solo usar BashOperator y EmptyOperator**
2. **NO usar PythonOperator ni funciones Python complejas**
3. **Todos los BashOperator deben usar echo con mensajes descriptivos**
4. **Todas las dependencias deben estar claramente definidas**
5. **Mensajes en español, palabras reservadas en inglés**
6. **Cada DAG en el mismo archivo `challenge.py`**

## Criterios de Éxito

- [ ] 8 DAGs creados con los dag_ids especificados
- [ ] Schedules correctos según especificación (presets y cron)
- [ ] Todos los DAGs con catchup=False excepto donde se indique
- [ ] Start dates correctos (hoy vs fechas específicas)
- [ ] Tags completos y correctos
- [ ] Todas las tareas implementadas con BashOperator/EmptyOperator
- [ ] Dependencias implementadas según especificación
- [ ] Uso de dependencias paralelas donde se especifica con []
- [ ] Mensajes descriptivos en español en cada echo
- [ ] Sin DAGs informativos adicionales

## Validación

Después de implementar, verifica:
1. Todos los DAGs aparecen en Airflow UI sin errores
2. Los schedules son correctos (verifica próxima ejecución)
3. Trigger manual de los DAGs funciona correctamente
4. Las dependencias se visualizan correctamente en Graph View
5. Puedes filtrar por tags 'delivery' para ver todos juntos

## Cálculo Esperado

- **DAGs programados**: 7 DAGs
- **DAGs manuales**: 1 DAG
- **Total**: 8 DAGs
- **Total de tareas**: Aproximadamente 54 tareas en total

¡Buena suerte! Este desafío simula un sistema real de data engineering para una startup.
"""

# TODO: Implementa los 8 DAGs según las especificaciones anteriores

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Implementa tu solución aquí...
