"""
TaskFlow - Usando logical_date para Procesamiento Temporal

Demuestra cómo usar logical_date (antes execution_date)
para procesar datos de una fecha/período específico.

Fundamental para ETLs con data intervals.
"""

import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator


@task
def extract_for_date(logical_date):
    """Extrae datos para la fecha lógica"""
    # logical_date es la fecha del DAG run
    date_str = logical_date.strftime('%Y-%m-%d')
    
    print(f"📥 Extrayendo datos para: {date_str}")
    print(f"  - Día de semana: {logical_date.strftime('%A')}")
    print(f"  - Mes: {logical_date.strftime('%B')}")
    print(f"  - Año: {logical_date.year}")
    
    # Simular extracción de datos para esta fecha
    data = {
        'date': date_str,
        'records': [
            {'id': 1, 'date': date_str, 'value': 100},
            {'id': 2, 'date': date_str, 'value': 200},
            {'id': 3, 'date': date_str, 'value': 150}
        ]
    }
    
    print(f"✅ Extraídos {len(data['records'])} registros")
    return data


@task
def extract_with_interval(data_interval_start, data_interval_end):
    """Extrae datos para un intervalo de tiempo"""
    print(f"📊 Procesando intervalo:")
    print(f"  - Inicio: {data_interval_start}")
    print(f"  - Fin: {data_interval_end}")
    
    # Para DAG @daily:
    # Si logical_date es 2024-01-15:
    #   data_interval_start = 2024-01-15 00:00:00
    #   data_interval_end = 2024-01-16 00:00:00
    
    # Query SQL típico
    start_str = data_interval_start.strftime('%Y-%m-%d %H:%M:%S')
    end_str = data_interval_end.strftime('%Y-%m-%d %H:%M:%S')
    
    query = f"""
    SELECT *
    FROM events
    WHERE timestamp >= '{start_str}'
      AND timestamp < '{end_str}'
    """
    
    print(f"📝 Query generado:")
    print(query)
    
    # Simular extracción
    hours_in_interval = (data_interval_end - data_interval_start).total_seconds() / 3600
    
    return {
        'interval_start': str(data_interval_start),
        'interval_end': str(data_interval_end),
        'hours': hours_in_interval,
        'query': query,
        'records_count': 1250
    }


@task
def process_with_date_logic(data: dict, logical_date):
    """Procesa datos con lógica específica de fecha"""
    print(f"⚙️ Procesando datos de {data['date']}")
    
    # Lógica basada en día de semana
    day_of_week = logical_date.weekday()  # 0=Monday, 6=Sunday
    
    if day_of_week in [5, 6]:  # Sábado o Domingo
        print("  📅 Fin de semana: aplicar lógica especial")
        multiplier = 1.5
    else:
        print("  📅 Día laboral: lógica normal")
        multiplier = 1.0
    
    # Procesar registros
    processed = []
    for record in data['records']:
        processed.append({
            **record,
            'adjusted_value': record['value'] * multiplier,
            'day_type': 'weekend' if day_of_week in [5, 6] else 'weekday'
        })
    
    print(f"✅ Procesados {len(processed)} registros")
    return {'date': data['date'], 'records': processed}


@task
def generate_partition_path(logical_date):
    """Genera path particionado por año/mes/día"""
    # Patrón común en data lakes
    year = logical_date.year
    month = logical_date.strftime('%m')
    day = logical_date.strftime('%d')
    
    path = f"s3://my-bucket/data/year={year}/month={month}/day={day}/"
    
    print(f"📂 Path particionado:")
    print(f"  {path}")
    
    # También útil para nombres de archivo
    filename = f"data_{logical_date.strftime('%Y%m%d')}.parquet"
    full_path = path + filename
    
    print(f"📄 Archivo: {filename}")
    
    return {
        'partition_path': path,
        'filename': filename,
        'full_path': full_path,
        'year': year,
        'month': month,
        'day': day
    }


with DAG(
    dag_id='taskflow_logical_date',
    schedule='@daily',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'context']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Pipeline con logical_date
    daily_data = extract_for_date()
    interval_data = extract_with_interval()
    processed = process_with_date_logic(daily_data)
    paths = generate_partition_path()
    
    end = EmptyOperator(task_id='end')
    
    start >> [daily_data, interval_data, paths]
    daily_data >> processed >> end
    interval_data >> end
    paths >> end

dag.doc_md = """
# logical_date en TaskFlow API

**logical_date**: Fecha lógica para la cual el DAG está procesando datos.

## ⚠️ Cambio importante en Airflow 3.x:

```python
# ❌ Airflow 2.x (deprecated)
execution_date

# ✅ Airflow 3.x
logical_date
```

## Concepto:

Para DAG @daily que corre a las 2 AM:
- **logical_date**: 2024-01-15 (fecha de los datos)
- **Run starts**: 2024-01-16 02:00 (cuando realmente ejecuta)

logical_date = fecha de los datos a procesar, NO cuando ejecuta.

## Uso básico:

```python
@task
def process_data(logical_date):
    date_str = logical_date.strftime('%Y-%m-%d')
    print(f"Processing data for {date_str}")
    
    # Query con esta fecha
    query = f"SELECT * FROM table WHERE date = '{date_str}'"
```

## Data intervals:

```python
@task
def process_interval(data_interval_start, data_interval_end):
    # Para DAG @daily el 2024-01-15:
    # data_interval_start: 2024-01-15 00:00:00
    # data_interval_end: 2024-01-16 00:00:00
    
    query = f\"\"\"
    SELECT * FROM events
    WHERE timestamp >= '{data_interval_start}'
      AND timestamp < '{data_interval_end}'
    \"\"\"
```

## Relación entre fechas:

```python
@task
def compare_dates(
    logical_date,
    data_interval_start,
    data_interval_end,
    next_execution_date,
    prev_execution_date
):
    print(f"logical_date: {logical_date}")
    print(f"data_interval_start: {data_interval_start}")
    print(f"data_interval_end: {data_interval_end}")
    print(f"next_execution_date: {next_execution_date}")
    print(f"prev_execution_date: {prev_execution_date}")
    
    # Para @daily schedule:
    # logical_date == data_interval_start
```

## Formatos comunes:

```python
@task
def format_dates(logical_date):
    # ISO format: 2024-01-15
    iso = logical_date.strftime('%Y-%m-%d')
    
    # For filenames: 20240115
    compact = logical_date.strftime('%Y%m%d')
    
    # For display: January 15, 2024
    readable = logical_date.strftime('%B %d, %Y')
    
    # For queries: 2024-01-15 00:00:00
    timestamp = logical_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Unix timestamp
    unix = int(logical_date.timestamp())
    
    return {
        'iso': iso,
        'compact': compact,
        'readable': readable,
        'timestamp': timestamp,
        'unix': unix
    }
```

## Particionado por fecha:

```python
@task
def partition_path(logical_date):
    # Hive-style partitioning
    year = logical_date.year
    month = logical_date.strftime('%m')
    day = logical_date.strftime('%d')
    
    # year=2024/month=01/day=15/
    path = f"year={year}/month={month}/day={day}/"
    
    # S3/GCS path
    s3_path = f"s3://bucket/data/{path}"
    
    return {'path': path, 's3_path': s3_path}
```

## Lógica condicional por fecha:

```python
@task
def date_based_logic(logical_date):
    # Día de semana (0=Monday, 6=Sunday)
    weekday = logical_date.weekday()
    
    # Fin de mes
    next_day = logical_date + timedelta(days=1)
    is_month_end = next_day.month != logical_date.month
    
    # Primer día del mes
    is_month_start = logical_date.day == 1
    
    # Lógica condicional
    if is_month_end:
        process_monthly_aggregates()
    
    if weekday in [5, 6]:  # Weekend
        use_reduced_capacity()
    
    if is_month_start:
        send_monthly_report()
```

## Lookback windows:

```python
@task
def process_with_lookback(logical_date):
    # Ventana de 7 días
    lookback_start = logical_date - timedelta(days=7)
    
    query = f\"\"\"
    SELECT *
    FROM events
    WHERE date >= '{lookback_start.strftime('%Y-%m-%d')}'
      AND date <= '{logical_date.strftime('%Y-%m-%d')}'
    \"\"\"
    
    # Útil para rolling aggregates
    return {'query': query}
```

## Manejo de timezones:

```python
from pendulum import datetime as pendulum_datetime

@task
def handle_timezones(logical_date):
    # logical_date ya es timezone-aware (pendulum)
    utc_time = logical_date.in_timezone('UTC')
    ny_time = logical_date.in_timezone('America/New_York')
    tokyo_time = logical_date.in_timezone('Asia/Tokyo')
    
    print(f"UTC: {utc_time}")
    print(f"New York: {ny_time}")
    print(f"Tokyo: {tokyo_time}")
```

## Testing con fechas específicas:

```python
@task
def testable_task(logical_date):
    # Código testable con cualquier fecha
    date_str = logical_date.strftime('%Y-%m-%d')
    data = fetch_data_for_date(date_str)
    return process(data)

# En tests:
# result = testable_task.function(logical_date=pendulum.datetime(2024, 1, 15))
```

## Backfill considerations:

```python
@task
def backfill_aware(logical_date, dag_run):
    # Detectar si es backfill
    is_backfill = dag_run.run_type == 'backfill'
    
    if is_backfill:
        print(f"Backfilling data for {logical_date}")
        # Puede usar lógica diferente para backfill
        use_bulk_mode = True
    else:
        use_bulk_mode = False
    
    return process_data(logical_date, bulk=use_bulk_mode)
```

## Common pitfalls:

```python
# ❌ NO usar datetime.now()
@task
def wrong_approach():
    today = datetime.now()  # ❌ Fecha de ejecución, no datos
    return process(today)

# ✅ Usar logical_date
@task
def correct_approach(logical_date):
    return process(logical_date)  # ✅ Fecha de datos

# ❌ NO hardcodear fechas
@task
def wrong_dates():
    date = '2024-01-15'  # ❌ Hardcoded
    return process(date)

# ✅ Usar logical_date dinámicamente
@task
def correct_dates(logical_date):
    date = logical_date.strftime('%Y-%m-%d')  # ✅ Dinámico
    return process(date)
```

## Best practices:

1. **Siempre usar logical_date**: No datetime.now()
2. **Data intervals para ranges**: start/end para períodos
3. **Timezone-aware**: Usa pendulum, no datetime nativo
4. **ISO format**: YYYY-MM-DD para consistencia
5. **Testeable**: Código debe funcionar con cualquier fecha
6. **Idempotente**: Misma fecha → mismo resultado

## Ejemplo completo:

```python
@task
def etl_daily_data(
    logical_date,
    data_interval_start,
    data_interval_end,
    ti
):
    # Formatear fecha para queries
    date_str = logical_date.strftime('%Y-%m-%d')
    
    # Log de intervalo procesado
    print(f"Processing interval:")
    print(f"  {data_interval_start} → {data_interval_end}")
    
    # Extract
    query = f\"\"\"
    SELECT *
    FROM events
    WHERE event_date = '{date_str}'
      AND timestamp >= '{data_interval_start}'
      AND timestamp < '{data_interval_end}'
    \"\"\"
    
    records = execute_query(query)
    
    # Transform (lógica basada en fecha)
    is_weekend = logical_date.weekday() in [5, 6]
    multiplier = 1.5 if is_weekend else 1.0
    
    transformed = [
        {**r, 'value': r['value'] * multiplier}
        for r in records
    ]
    
    # Load (particionado por fecha)
    partition = f"year={logical_date.year}/month={logical_date.month:02d}/day={logical_date.day:02d}"
    output_path = f"s3://bucket/data/{partition}/"
    
    # Metadata
    ti.xcom_push(key='metadata', value={
        'date': date_str,
        'interval': f"{data_interval_start} to {data_interval_end}",
        'records': len(transformed),
        'output_path': output_path,
        'is_weekend': is_weekend
    })
    
    return {
        'records': len(transformed),
        'path': output_path
    }
```
"""
