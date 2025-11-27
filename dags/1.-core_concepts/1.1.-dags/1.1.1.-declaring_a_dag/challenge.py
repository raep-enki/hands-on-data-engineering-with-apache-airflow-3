"""
# DESAFÍO: Pipeline de Monitoreo y Reportes de Sitio Web

## Objetivo
Crear un DAG que monitorea la salud de un sitio web y genera reportes usando solo
operadores básicos y dependencias de tareas. Este desafío se enfoca en la estructura
del DAG y orquestación de tareas sin características avanzadas.

## Requisitos

### 1. Configuración del DAG
- **DAG ID**: `website_monitoring_pipeline`
- **Schedule**: Cada 30 minutos (`*/30 * * * *`)
- **Start Date**: 1 de enero de 2024
- **Catchup**: Deshabilitado
- **Tags**: `['challenge', 'core_concepts', 'dags', 'declaring_a_dag']`
- **Description**: "Monitorear salud del sitio web y generar reportes"

### 2. Usar Estilo Context Manager
Usa el patrón `with DAG(...)` context manager para declarar tu DAG.

### 3. Tareas Requeridas

Crea las siguientes tareas usando **solo BashOperator** y **EmptyOperator**:

**Tarea 1: start_monitoring** (EmptyOperator)
- Marca el inicio del ciclo de monitoreo

**Tarea 2: ping_website** (BashOperator)
- Comando: `echo "Haciendo ping al sitio web - verificando respuesta"`

**Tarea 3: check_http_status** (BashOperator)
- Comando: `echo "Verificando código de estado HTTP"`

**Tarea 4: check_response_time** (BashOperator)
- Comando: `echo "Midiendo tiempo de respuesta"`

**Tarea 5: check_ssl_certificate** (BashOperator)
- Comando: `echo "Verificando validez del certificado SSL"`

**Tarea 6: health_checkpoint** (EmptyOperator)
- Punto de control después de que todas las verificaciones se completen

**Tarea 7: analyze_uptime** (BashOperator)
- Comando: `echo "Calculando porcentaje de tiempo activo"`

**Tarea 8: analyze_performance** (BashOperator)
- Comando: `echo "Analizando métricas de rendimiento"`

**Tarea 9: generate_alerts** (BashOperator)
- Comando: `echo "Generando alertas para problemas"`

**Tarea 10: update_dashboard** (BashOperator)
- Comando: `echo "Actualizando dashboard de monitoreo"`

**Tarea 11: send_report** (BashOperator)
- Comando: `echo "Enviando reporte de monitoreo"`

**Tarea 12: end_monitoring** (EmptyOperator)
- Marca el fin del ciclo de monitoreo

### 4. Dependencias de Tareas

Implementa la siguiente estructura de dependencias:

```
start_monitoring
    ↓
ping_website
    ↓
[check_http_status, check_response_time, check_ssl_certificate] (paralelo)
    ↓
health_checkpoint
    ↓
[analyze_uptime, analyze_performance] (paralelo)
    ↓
generate_alerts
    ↓
[update_dashboard, send_report] (paralelo)
    ↓
end_monitoring
```

### 5. Guías de Implementación

- Usa **solo** `BashOperator` y `EmptyOperator`
- NO uses `PythonOperator` ni funciones
- NO uses XComs ni paso de datos
- Usa el operador `>>` para definir dependencias
- Usa sintaxis `[task1, task2]` para ejecución paralela
- Incluye `task_id` para cada tarea
- Referencia los imports proporcionados abajo

### 6. Imports Requeridos

```python
import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
```

### 7. Desafíos Bonus

1. Agregar una tarea de limpieza que se ejecute después de que todo se complete
2. Crear una segunda rama paralela para verificaciones de salud de base de datos
3. Agregar tareas de punto de control entre cada etapa principal
4. Usar pendulum en lugar de datetime para el start_date

## Criterios de Éxito

- [ ] El DAG aparece en la UI de Airflow sin errores
- [ ] Las 12 tareas son visibles en la vista de grafo
- [ ] Las dependencias de tareas coinciden con la estructura especificada
- [ ] Las tareas paralelas se muestran claramente en el grafo
- [ ] El DAG puede ser disparado manualmente y todas las tareas se vuelven verdes
- [ ] Los EmptyOperators se usan como marcadores de flujo de trabajo
- [ ] Los BashOperators usan comandos echo simples

## Consejos

- Comienza creando el DAG con context manager
- Crea todas las tareas primero, luego define dependencias
- Usa listas `[]` para tareas que deben ejecutarse en paralelo
- Prueba la vista de grafo para verificar tu estructura de dependencias
- Recuerda: Manténlo simple - ¡no se necesitan funciones Python!

## Ejemplo de Creación de Tarea

```python
my_task = BashOperator(
    task_id='my_task_id',
    bash_command='echo "Descripción de la tarea"'
)
```

## Ejemplo de Dependencias

```python
# Lineal
task1 >> task2 >> task3

# Fan-out paralelo
task1 >> [task2, task3, task4]

# Fan-in paralelo
[task2, task3, task4] >> task5

# Complejo
start >> task1 >> [task2, task3] >> task4 >> end
```

¡Buena suerte! Enfócate en obtener la estructura correcta - esto se trata de
declaración de DAG, no de lógica compleja.
"""

# TODO: Implementa tu solución abajo
# Recuerda: ¡Solo usa BashOperator y EmptyOperator, sin funciones Python!
