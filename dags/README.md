# Apache Airflow 3.1.3 - Guía del Curso

## 📚 Estructura del Curso

Los ejemplos están organizados en **7 niveles pedagógicos**, desde conceptos básicos hasta patrones avanzados:

### 📘 NIVEL 1: Fundamentos de DAGs
*Conceptos básicos para crear y ejecutar DAGs*

| # | Tema | Ejemplos | Challenge | Descripción |
|---|------|----------|-----------|-------------|
| 01 | [declaring_a_dag](01.-declaring_a_dag/) | 2 | ✓ | Declaración básica de DAGs (context manager, pendulum) |
| 02 | [default_arguments](02.-default_arguments/) | 3 | ✓ | Argumentos por defecto y configuración base |
| 03 | [running_dags](03.-running_dags/) | 3 | ✓ | Scheduling (presets, cron, manual) |
| 04 | [documentation](04.-documentation/) | 3 | ✓ | Documentación de DAGs con Markdown |

### 🔗 NIVEL 2: Relaciones entre Tareas
*Control de flujo sin paso de datos - usando operadores básicos*

| # | Tema | Ejemplos | Challenge | Descripción |
|---|------|----------|-----------|-------------|
| 05 | [relationships](05.-relationships/) | 3 | ✓ | Relaciones entre tareas (bitshift, chain, parallel) |
| 06 | [branching](06.-branching/) | 3 | ✓ | Ramificación condicional con BranchPythonOperator |
| 07 | [trigger_rules](07.-trigger_rules/) | 5 | ✓ | Reglas de activación (all_success, one_success, etc.) |
| 08 | [setup_and_teardown](08.-setup_and_teardown/) | 3 | ✓ | Patrones setup/teardown para recursos |

### ⏰ NIVEL 3: Gestión de Ejecuciones
*Control avanzado de cuándo y cómo se ejecutan los DAGs*

| # | Tema | Ejemplos | Challenge | Descripción |
|---|------|----------|-----------|-------------|
| 09 | [timeouts](09.-timeouts/) | 3 | ✓ | Timeouts de ejecución y sensores |
| 10 | [catchup](10.-catchup/) | 3 | ✓ | Catchup de ejecuciones pasadas |
| 11 | [backfill](11.-backfill/) | 3 | ✓ | Backfill de datos históricos |
| 12 | [latest_only](12.-latest_only/) | 2 | ✓ | Ejecución solo de última instancia |
| 13 | [depends_on_past](13.-depends_on_past/) | 0 | - | Dependencia de ejecuciones pasadas |
| 14 | [deadline_alerts](14.-deadline_alerts/) | 2 | ✓ | Alertas de deadline y SLA |

### 🎨 NIVEL 4: Organización y Estructura
*Mejores prácticas de organización y visualización*

| # | Tema | Ejemplos | Challenge | Descripción |
|---|------|----------|-----------|-------------|
| 15 | [task_groups](15.-task_groups/) | 4 | ✓ | Agrupación lógica de tareas |
| 16 | [edge_labels](16.-edge_labels/) | 3 | ✓ | Etiquetas en dependencias |
| 17 | [loading_dags](17.-loading_dags/) | 3 | ✓ | Patrones de carga de DAGs |
| 18 | [packaging_dags](18.-packaging_dags/) | 4 | ✓ | Empaquetado y estructura de código |
| 19 | [dynamic_dags](19.-dynamic_dags/) | 3 | ✓ | Generación dinámica de DAGs |

### 🚀 NIVEL 5: TaskFlow API y Comunicación
*Introducción a TaskFlow API y paso de datos entre tareas*

| # | Tema | Ejemplos | Challenge | Descripción |
|---|------|----------|-----------|-------------|
| 20 | [context](20.-context/) | 3 | ✓ | TaskFlow API, decorador @task, context |
| 21 | [xcoms](21.-xcoms/) | 4 | ✓ | XComs para comunicación entre tareas |
| 22 | [passing_arbitrary_objects_as_arguments](22.-passing_arbitrary_objects_as_arguments/) | 3 | ✓ | Paso de objetos complejos |

### ⚙️ NIVEL 6: Configuración Externa
*Variables y parámetros para configuración dinámica*

| # | Tema | Ejemplos | Challenge | Descripción |
|---|------|----------|-----------|-------------|
| 23 | [variables](23.-variables/) | 4 | ✓ | Variables de Airflow |
| 24 | [params](24.-params/) | 4 | ✓ | Parámetros de DAG y runtime |

### ⚡ NIVEL 7: Patrones Avanzados
*Técnicas avanzadas de Airflow 3.x*

| # | Tema | Ejemplos | Challenge | Descripción |
|---|------|----------|-----------|-------------|
| 25 | [simple_mapping](25.-simple_mapping/) | 3 | ✓ | Dynamic Task Mapping (expand, partial) |
| 26 | [asset_definitions](26.-asset_definitions/) | 3 | ✓ | Asset Definitions para data-aware scheduling |

## 🎓 Progresión Pedagógica

```
NIVEL 1 (Fundamentos)
   ↓
NIVEL 2 (Control de Flujo - Sin TaskFlow)
   ↓
NIVEL 3 (Gestión de Ejecuciones)
   ↓
NIVEL 4 (Organización de Código)
   ↓
NIVEL 5 (TaskFlow API - Con paso de datos)
   ↓
NIVEL 6 (Configuración Externa)
   ↓
NIVEL 7 (Patrones Avanzados)
```

**Rationale:**
1. **Nivel 1 → 2**: Fundamentos antes de control de flujo
2. **Nivel 2 → 3**: Control básico antes de gestión avanzada
3. **Nivel 3 → 4**: Ejecución antes de organización
4. **Nivel 4 → 5**: Operadores básicos antes de TaskFlow API
5. **Nivel 5 → 6**: Paso de datos antes de configuración
6. **Nivel 6 → 7**: Configuración antes de patrones avanzados

## ⚠️ Restricciones por Nivel

### Antes del Nivel 5 (TaskFlow API)

**❌ NO USAR**:
- Decorador `@task`
- Decorador `@task.branch`
- Decorador `@task_group`
- XComs (excepto en tema 21)
- Variables (excepto en tema 23)
- Params (excepto en tema 24)

**✅ USAR**:
- `BashOperator`
- `EmptyOperator`
- `PythonOperator`
- `BranchPythonOperator`

### A partir del Nivel 5

Se introduce TaskFlow API con `@task` y manejo automático de XComs.

## 🏗️ Convenciones de Airflow 3.x

### Imports Obligatorios
```python
from airflow.sdk import DAG, task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
```

### Propiedades Obligatorias de DAGs
```python
with DAG(
    dag_id='unique_dag_id',
    start_date=datetime.datetime(2024, 1, 1),  # o pendulum.datetime()
    schedule='@daily',  # o cron expression
    catchup=False,
    tags=['example', 'nombre_del_tema']
):
    # tareas...
```

### Context en Airflow 3.x
```python
# Airflow 3.x
logical_date = context['logical_date']

# Airflow 2.x (deprecated)
execution_date = context['execution_date']
```

## 📋 Guía para Desafíos (Challenges)

Cada tema incluye un archivo `challenge.py` con:
- **Objetivo**: Descripción del problema a resolver
- **Requisitos**: Constraints y reglas a seguir
- **Pistas**: Guías para la solución

**No incluyen la solución** - están diseñados para practicar los conceptos del tema.

## 🚀 Iniciar el Entorno

```bash
# Desde la raíz del proyecto
docker compose up
```

**Acceder a la UI**: http://localhost:8080
- Usuario: `admin`
- Contraseña: `admin`

**Tips**:
- Los DAGs se refrescan cada 15 segundos
- Filtrar por tags en la UI para ver ejemplos por tema
- Revisar logs en la carpeta `logs/`

---

**Total**: 26 temas | 79 ejemplos | 25 challenges | Apache Airflow 3.1.3
