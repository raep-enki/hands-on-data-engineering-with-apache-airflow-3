# Copilot Instructions for Apache Airflow 3.x Learning Repository

## Project Overview

This is an educational repository demonstrating Apache Airflow 3.1.3 concepts through hands-on examples. The codebase is organized as a progression of learning modules covering core concepts and authoring patterns.

## Critical Architecture Patterns

### Airflow 3.x SDK Usage
- **Always use** `from airflow.sdk import DAG, task, task_group` (not `from airflow import DAG`)
- Standard operators come from `airflow.providers.standard.operators.*`
- **BranchPythonOperator**: Import from `airflow.providers.standard.operators.python` (not `airflow.operators.python`)
- Example:
  ```python
  from airflow.sdk import DAG, task
  from airflow.providers.standard.operators.bash import BashOperator
  from airflow.providers.standard.operators.python import BranchPythonOperator
  ```

### DAG Declaration Styles
Two primary patterns are used:
1. **Context manager style** (preferred for new DAGs):
   ```python
   with DAG(dag_id='my_dag', start_date=datetime.datetime(2021, 1, 1), schedule='@daily'):
       task1 = EmptyOperator(task_id='task')
   ```

2. **Explicit instance style** (for complex control flow):
   ```python
   dag = DAG('my_dag', schedule='@daily')
   task = BashOperator(task_id='foo', bash_command='echo hi', dag=dag)
   ```

### Date/Time Handling
- Use `pendulum` library for timezone-aware datetimes: `pendulum.datetime(2021, 1, 1, tz='UTC')`
- Plain `datetime.datetime` is acceptable for simple cases without timezone requirements
- **Airflow 3.x Context Change**: Use `logical_date` instead of `execution_date`
  ```python
  def my_callable(**context):
      logical_date = context['logical_date']  # NOT execution_date
  ```

## DAG Structure & Organization

### Directory Taxonomy
Examples are hierarchically organized by topic:
- `dags/1.-core_concepts/1.1.-dags/1.1.X.-topic_name/exampleN.py`
- Each subdirectory contains isolated, runnable DAG examples
- **Naming Convention**: Use `example1.py`, `example2.py`, ..., `example5.py` for examples
- Use `challenge.py` for practice exercises

### Mandatory DAG Properties
Every DAG must include:
- `dag_id`: Unique identifier
- `start_date`: First execution date
- `schedule`: Cron expression or preset (`@daily`, `@hourly`, etc.)
- `tags`: List of hierarchical tags matching directory structure (e.g., `['example', 'core_concepts', 'dags', 'documentation']`)

### Tagging Convention
Tags follow the directory path pattern:
```python
tags=['example', 'section_name', 'subsection', 'specific_topic']
```
This enables filtering DAGs in the Airflow UI by learning module.

## TaskFlow API & Control Flow

### Task Decorators
- Use `@task` for Python callables, `@task.branch` for branching logic
- Task groups use `@task_group()` decorator
- Example branching:
  ```python
  @task.branch(task_id='branch_task')
  def branch_func(ti=None):
      xcom_value = int(ti.xcom_pull(task_ids='start_task'))
      return 'task_id_to_run' if xcom_value >= 5 else None
  ```

### Setup/Teardown Pattern
Use `.as_teardown(setups=setup_task)` for resource cleanup:
```python
s1 = EmptyOperator(task_id='setup')
t1 = EmptyOperator(task_id='teardown').as_teardown(setups=s1)
s1 >> work_task >> t1
```

## Local Development Environment

### Running Airflow Standalone
```bash
# Docker Compose setup with standalone mode
docker compose up
# Access UI at http://localhost:8080
# Credentials: admin/admin (from AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS)
```

### Key Configuration (.env)
- `AIRFLOW_IMAGE_NAME=apache/airflow:3.1.3-python3.12`
- `AIRFLOW__CORE__EXECUTOR=LocalExecutor`
- DAGs refresh every 15 seconds (`AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=15`)
- Example DAGs disabled (`AIRFLOW__CORE__LOAD_EXAMPLES=False`)
- Simple auth manager with passwords file in `config/`

### Volume Mounts
- `./dags` → `/opt/airflow/dags`
- `./logs` → `/opt/airflow/logs`
- `./config` → `/opt/airflow/config`
- `./plugins` → `/opt/airflow/plugins` (currently unused)

## Common Patterns & Anti-Patterns

### Documentation
DAGs support inline documentation via `dag.doc_md` and `task.doc_md` (accepts markdown):
```python
dag.doc_md = __doc__  # Use module docstring
task.doc_md = '''# Title\nMarkdown content'''
```

### Dynamic DAG Generation
Loop over collections to create dynamic task graphs:
```python
options = ['branch_a', 'branch_b', 'branch_c']
for option in options:
    t = EmptyOperator(task_id=option)
    first >> t >> last
```

### Default Arguments
Set task defaults at DAG level:
```python
DAG(dag_id='my_dag', default_args={'retries': 2})
# All tasks inherit retries=2 unless overridden
```

## Testing & Validation

No formal test suite exists. Validate DAGs by:
1. Starting Airflow: `docker compose up`
2. Check DAG appears in UI without import errors
3. Manually trigger test runs
4. Review logs in `logs/dag_id=<dag_id>/`

## Adding New Examples

When creating new DAG examples:
1. Place in appropriate hierarchical directory under `dags/`
2. Name files as `example1.py`, `example2.py`, ..., `example5.py` (5 examples per topic)
3. Name practice files as `challenge.py`
4. Include all mandatory properties (dag_id, start_date, schedule, tags)
5. Set `catchup=False` to prevent backfilling on first load
6. Use descriptive tags matching the directory structure
7. **Do NOT create README.md files** - only Python DAG files

## Curriculum Structure & Prerequisites

### Topic Order
The curriculum follows a strict progression:
1. **1.1 - DAGs**: Basic DAG concepts, control flow, documentation
2. **1.2 - DAG Runs**: Catchup, backfill
3. **1.3 - Tasks**: Relationships, timeouts
4. **1.4 - TaskFlow**: TaskFlow API, context, passing objects
5. **1.5 - XComs**: Cross-communication between tasks
6. **1.6 - Variables**: Airflow Variables
7. **1.7 - Params**: DAG and task parameters
8. **2.1 - Dynamic Task Mapping**: Advanced patterns
9. **2.2 - Asset Definitions**: Data-aware scheduling

### Prerequisite Enforcement
- **Do NOT use TaskFlow API** (`@task`, `@task.branch`, `@task_group`) before section 1.4
- **Do NOT use XComs** before section 1.5
- **Do NOT use Variables** before section 1.6
- **Do NOT use Params** before section 1.7
- In early sections (1.1-1.3), only use: `BashOperator`, `EmptyOperator`, `BranchPythonOperator`, `PythonOperator`

### Challenge Format
- Each `challenge.py` must request **exactly ONE DAG** (unless the topic is specifically about multiple DAGs)
- Provide clear instructions in the docstring
- Include requirements and constraints
- Do not provide the solution - only the problem statement
