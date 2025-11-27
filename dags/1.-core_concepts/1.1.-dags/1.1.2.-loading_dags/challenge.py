"""
# DESAFÍO: Sistema de Procesamiento Multi-Región con Factory Pattern

## Objetivo
Crear un sistema de DAGs que procese datos para múltiples regiones y departamentos
usando patrones de carga dinámica, factory functions y carga condicional.

## Requisitos

### 1. Configuración Base

Define las siguientes estructuras de datos:

```python
# Regiones a procesar
REGIONES = ['norte', 'sur', 'centro', 'caribe']

# Departamentos por región
DEPARTAMENTOS = {
    'ventas': {'schedule': '@hourly', 'prioridad': 'alta'},
    'marketing': {'schedule': '0 */6 * * *', 'prioridad': 'media'},
    'operaciones': {'schedule': '@daily', 'prioridad': 'media'},
    'finanzas': {'schedule': '0 2 * * *', 'prioridad': 'alta'}
}

# Variable de entorno para control
MODO = os.getenv('PROCESAMIENTO_MODO', 'completo')  # 'completo' o 'solo_criticos'
```

### 2. Función Factory Requerida

Crea una función `crear_dag_regional()` que:
- Acepte parámetros: `region`, `departamento`, `schedule`, `prioridad`
- Retorne un DAG configurado
- DAG ID formato: `procesar_{region}_{departamento}`
- Description: "Procesar datos de {departamento} para región {region}"
- Tags: `['challenge', 'core_concepts', 'dags', 'loading_dags', region, departamento, prioridad]`

La función debe crear las siguientes tareas (usar solo BashOperator y EmptyOperator):
1. **inicio** (EmptyOperator)
2. **conectar_sistema** (BashOperator) - mensaje: "Conectando a sistema de {departamento}"
3. **extraer_datos_regionales** (BashOperator) - mensaje: "Extrayendo datos de {region}"
4. **validar_datos** (BashOperator) - mensaje: "Validando datos de {departamento}"
5. **procesar_datos** (BashOperator) - mensaje: "Procesando datos de {region} - {departamento}"
6. **generar_reporte** (BashOperator) - mensaje: "Generando reporte para {region}"
7. **fin** (EmptyOperator)

Dependencias: inicio >> conectar >> extraer >> validar >> procesar >> generar >> fin

### 3. Generación Dinámica de DAGs

Usa bucles anidados para:
- Iterar sobre todas las regiones
- Iterar sobre todos los departamentos
- Llamar a la función factory para cada combinación
- Asignar cada DAG a `globals()` con el nombre correcto

### 4. Carga Condicional

Si `MODO == 'solo_criticos'`:
- Solo crear DAGs donde `prioridad == 'alta'`
- Agregar tag adicional: 'critico'

Si `MODO == 'completo'`:
- Crear DAGs para todos los departamentos

### 5. DAG de Resumen

Crear un DAG manual llamado `resumen_procesamiento_regional` que:
- No tenga schedule (trigger manual)
- Tenga una tarea BashOperator que muestre:
  * Total de regiones configuradas
  * Total de departamentos configurados
  * Modo de operación actual
  * Lista de todas las combinaciones de DAGs creados
  * Conteo de DAGs críticos vs no críticos

### 6. DAG de Monitoreo por Región

Para cada región, crear un DAG adicional `monitoreo_{region}` que:
- Schedule: `0 8 * * *` (8 AM diario)
- Tenga tareas que muestren el estado de todos los departamentos de esa región
- Use solo BashOperator con echo para mostrar información

## Estructura Esperada de Archivos

Todo debe estar en un solo archivo: `challenge.py`

## Cálculo Esperado de DAGs

- Regiones: 4
- Departamentos: 4
- DAGs principales: 4 × 4 = 16 (o menos si modo='solo_criticos')
- DAGs de monitoreo regional: 4
- DAG de resumen: 1
- **Total en modo 'completo': 21 DAGs**
- **Total en modo 'solo_criticos': 13 DAGs** (8 críticos + 4 monitoreo + 1 resumen)

## Restricciones Importantes

- NO usar PythonOperator ni funciones Python complejas
- NO usar XComs ni paso de datos entre tareas
- Solo usar conceptos hasta 1.1.2 (declaración + carga)
- Usar solo BashOperator (con echo) y EmptyOperator
- Todos los mensajes en español
- Palabras reservadas en inglés

## Criterios de Éxito

- [ ] Función factory correctamente implementada
- [ ] Todos los DAGs se generan dinámicamente
- [ ] Los DAGs aparecen en Airflow UI sin errores
- [ ] Carga condicional funciona según MODO
- [ ] Cada DAG tiene exactamente 7 tareas con dependencias correctas
- [ ] Tags incluyen región, departamento y prioridad
- [ ] DAG de resumen muestra información correcta
- [ ] DAGs de monitoreo regional funcionan
- [ ] Cambiar MODO afecta los DAGs cargados

## Ejemplo de Salida del DAG de Resumen

```
=== Resumen del Sistema de Procesamiento Regional ===

Configuración:
  Regiones: 4 (norte, sur, centro, caribe)
  Departamentos: 4 (ventas, marketing, operaciones, finanzas)
  Modo: completo

DAGs Generados:
  Críticos: 8 DAGs (prioridad alta)
  No Críticos: 8 DAGs (prioridad media)
  Monitoreo: 4 DAGs (uno por región)
  Total: 21 DAGs

Combinaciones:
  ✓ procesar_norte_ventas (crítico)
  ✓ procesar_norte_marketing
  ✓ procesar_norte_operaciones
  ... (continúa para todas las combinaciones)
```

## Consejos

- Usa f-strings para construir dag_ids y mensajes
- Recuerda que `globals()[nombre_variable] = valor` registra el DAG
- Los bucles anidados son: `for region in REGIONES: for dept in DEPARTAMENTOS.keys():`
- Para condicional: `if MODO == 'solo_criticos' and config['prioridad'] == 'alta':`
- Usa `**CONFIGURACION_COMUN` para evitar repetir parámetros

## Validación

Para verificar tu solución:
1. El archivo debe cargar sin errores en Airflow
2. Contar DAGs en UI debe dar 21 (o 13 en modo crítico)
3. Buscar por tags debe mostrar agrupaciones correctas
4. Trigger manual del resumen debe mostrar información precisa
5. Cada DAG individual debe tener 7 tareas

¡Buena suerte! Este desafío integra todos los conceptos de carga de DAGs.
"""

# TODO: Implementa tu solución completa aquí
# Incluye imports, configuraciones, función factory, generación de DAGs y DAGs auxiliares

import datetime
import os

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Agrega tu código aquí...
