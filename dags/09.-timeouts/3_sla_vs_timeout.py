"""
Timeouts - SLA (Service Level Agreement)

Demuestra el uso de sla para alertar cuando tareas tardan más de lo esperado.
SLA NO mata la tarea, solo genera alertas.

Diferencia con execution_timeout: SLA alerta, timeout mata.
"""

import datetime
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id='timeouts_sla',
    schedule='@hourly',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'timeouts']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Tarea con SLA de 5 minutos
    # Si tarda más, se envía alerta (pero continúa ejecutándose)
    critical_task = BashOperator(
        task_id='critical_task',
        bash_command='echo "🚨 Tarea crítica (SLA: 5 min)"; sleep 2',
        sla=timedelta(minutes=5)  # Alerta si excede 5 minutos
    )
    
    # Tarea con SLA y execution_timeout
    # SLA alerta, timeout mata
    monitored_task = BashOperator(
        task_id='monitored_task',
        bash_command='echo "⏱️ Monitoreada (SLA: 3 min, Timeout: 10 min)"; sleep 2',
        sla=timedelta(minutes=3),                # Alerta después de 3 min
        execution_timeout=timedelta(minutes=10)  # Mata después de 10 min
    )
    
    # Tarea normal sin SLA
    normal_task = BashOperator(
        task_id='normal_task',
        bash_command='echo "✅ Tarea normal (sin SLA)"; sleep 1'
    )
    
    # Tarea con SLA más estricto
    fast_task = BashOperator(
        task_id='fast_task',
        bash_command='echo "⚡ Debe ser rápida (SLA: 1 min)"; sleep 1',
        sla=timedelta(minutes=1)
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> [critical_task, monitored_task] >> normal_task >> fast_task >> end

dag.doc_md = """
# SLA (Service Level Agreement)

**SLA**: Define tiempo máximo esperado para una tarea. Si excede, genera alerta.

## Diferencia crucial: SLA vs Timeout

| Aspecto | SLA | execution_timeout |
|---------|-----|-------------------|
| **Propósito** | Monitoreo y alertas | Control de ejecución |
| **Acción** | Envía notificación | Mata el proceso |
| **Tarea continúa** | Sí ✅ | No ❌ |
| **Uso** | Medir performance | Evitar hangs |
| **Resultado** | Success + alerta | Failed |

## Configuración:

```python
task = BashOperator(
    task_id='my_task',
    bash_command='...',
    sla=timedelta(minutes=30)  # Alertar si excede 30 minutos
)
```

## Escenarios:

### Escenario 1: Tarea completa dentro de SLA
```
Inicio: 10:00
Fin: 10:25 (25 minutos)
SLA: 30 minutos
Resultado: Success ✅ (sin alerta)
```

### Escenario 2: Tarea excede SLA pero completa
```
Inicio: 10:00
SLA exceeded: 10:30 (30 minutos) → Alerta enviada 🚨
Fin: 10:45 (45 minutos)
Resultado: Success ✅ (con alerta de SLA)
```

### Escenario 3: SLA + Timeout
```python
task = BashOperator(
    task_id='my_task',
    sla=timedelta(minutes=30),          # Alerta después de 30 min
    execution_timeout=timedelta(hours=1) # Mata después de 60 min
)

# Timeline:
10:00 - Tarea inicia
10:30 - SLA excedido → Alerta enviada 🚨
10:45 - Tarea continúa ejecutándose...
11:00 - Timeout alcanzado → Tarea matada ❌
```

## Configuración de alertas SLA:

### 1. Email notification (airflow.cfg)
```ini
[email]
email_backend = airflow.providers.sendgrid.utils.emailer.send_email
smtp_host = smtp.sendgrid.net
smtp_starttls = True
smtp_ssl = False
smtp_user = apikey
smtp_password = your_sendgrid_api_key
smtp_port = 587
smtp_mail_from = airflow@example.com
```

### 2. SLA callback en DAG
```python
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    \"\"\"
    Función llamada cuando se pierde un SLA.
    
    Args:
        dag: DAG que contiene la tarea
        task_list: Lista de tareas que perdieron SLA
        blocking_task_list: Tareas que bloquean las que perdieron SLA
        slas: Lista de objetos SlaMiss
        blocking_tis: TaskInstances bloqueantes
    \"\"\"
    print(f"❌ SLA Miss!")
    for task in task_list:
        print(f"  - Task: {task.task_id}")
    
    # Enviar a Slack, PagerDuty, etc.
    send_slack_alert(f"SLA missed for tasks: {[t.task_id for t in task_list]}")

with DAG(
    dag_id='sla_with_callback',
    sla_miss_callback=sla_miss_callback,
    ...
):
    task = BashOperator(
        task_id='critical',
        sla=timedelta(minutes=5)
    )
```

## Casos de uso:

### ETL crítico (SLA estricto)
```python
extract = BashOperator(
    task_id='extract',
    sla=timedelta(minutes=10),
    execution_timeout=timedelta(minutes=30)
)
```

### Reportes de negocio (SLA business-driven)
```python
# Reporte debe estar listo a las 8 AM
morning_report = BashOperator(
    task_id='morning_report',
    sla=timedelta(hours=1),  # Máximo 1 hora
    start_date=datetime(2021, 1, 1, 7, 0)  # Inicia a las 7 AM
)
```

### APIs tiempo real (SLA muy estricto)
```python
api_update = BashOperator(
    task_id='api_update',
    sla=timedelta(seconds=30),  # Debe ser rapidísimo
    execution_timeout=timedelta(minutes=1)
)
```

## Monitoring de SLA:

### En Airflow UI:
- **Browse → SLA Misses**: Ver todas las violaciones de SLA
- **DAG Details**: Badge rojo si hay SLA miss
- **Task Instance**: Muestra si SLA fue excedido

### Queries útiles:
```python
# Ver SLA misses en base de datos
SELECT 
    dag_id,
    task_id,
    execution_date,
    timestamp as sla_miss_time
FROM sla_miss
WHERE timestamp > NOW() - INTERVAL '7 days'
ORDER BY timestamp DESC;
```

## SLA a nivel de DAG:

```python
# Todas las tareas heredan SLA de 30 minutos
default_args = {
    'sla': timedelta(minutes=30)
}

with DAG(
    dag_id='dag_with_sla',
    default_args=default_args,
    ...
):
    # task1 hereda SLA de 30 minutos
    task1 = BashOperator(task_id='task1', ...)
    
    # task2 override a 10 minutos
    task2 = BashOperator(
        task_id='task2', 
        sla=timedelta(minutes=10),
        ...
    )
    
    # task3 sin SLA
    task3 = BashOperator(
        task_id='task3',
        sla=None,
        ...
    )
```

## Best practices:

1. **SLA basado en negocio**: No técnico, sino requisito de negocio
2. **Buffer razonable**: SLA > duración promedio + margen
3. **Monitoreo proactivo**: Alertar antes de fallar
4. **Timeout > SLA**: execution_timeout debe ser mayor que SLA
5. **Escalamiento**: SLA misses críticos escalan a managers

## Ejemplo completo:

```python
def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    for task in task_list:
        send_pagerduty_alert(
            severity='warning',
            summary=f'SLA missed: {task.task_id}',
            details=f'Task exceeded expected duration'
        )

with DAG(
    dag_id='production_etl',
    schedule='0 6 * * *',  # 6 AM diario
    sla_miss_callback=notify_sla_miss,
    default_args={
        'sla': timedelta(hours=1),  # Todo debe completar en 1 hora
        'email_on_failure': True,
        'email': ['data-team@company.com']
    }
):
    
    extract = BashOperator(
        task_id='extract',
        bash_command='python extract.py',
        sla=timedelta(minutes=15),          # Estricto
        execution_timeout=timedelta(minutes=30)
    )
    
    transform = BashOperator(
        task_id='transform',
        bash_command='python transform.py',
        sla=timedelta(minutes=30),          # Moderado
        execution_timeout=timedelta(hours=1)
    )
    
    load = BashOperator(
        task_id='load',
        bash_command='python load.py',
        sla=timedelta(minutes=15),          # Estricto
        execution_timeout=timedelta(minutes=30)
    )
    
    extract >> transform >> load
```

## Tips:

1. Empezar sin SLA, medir duración real
2. Establecer SLA = P95 de duración histórica
3. Revisar SLA misses semanalmente
4. Ajustar SLAs según cambios en datos
5. Escalar alerts: warning → critical
"""
