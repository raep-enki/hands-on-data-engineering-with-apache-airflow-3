"""
Generación Dinámica con Bucle - Múltiples DAGs Similares

Demuestra cómo generar múltiples DAGs usando un bucle.
Cada DAG es asignado a globals() para que sea descubierto.
"""

import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Lista de regiones para las que queremos crear DAGs
regiones = ['norte', 'sur', 'este', 'oeste']

# Generar un DAG para cada región
for region in regiones:
    # Crear un ID único para cada DAG
    dag_id = f'procesar_ventas_{region}'
    
    # Crear el DAG
    dag = DAG(
        dag_id=dag_id,
        start_date=datetime.datetime(2024, 1, 1),
        schedule='@daily',
        catchup=False,
        description=f'Procesar ventas de la región {region}',
        tags=['example', 'core_concepts', 'dags', 'loading_dags', region]
    )
    
    # Crear tareas para este DAG
    with dag:
        inicio = EmptyOperator(task_id='inicio')
        
        extraer = BashOperator(
            task_id='extraer_datos',
            bash_command=f'echo "Extrayendo datos de ventas - Región: {region}"'
        )
        
        procesar = BashOperator(
            task_id='procesar_datos',
            bash_command=f'echo "Procesando ventas de {region}"'
        )
        
        cargar = BashOperator(
            task_id='cargar_datos',
            bash_command=f'echo "Cargando resultados de {region}"'
        )
        
        fin = EmptyOperator(task_id='fin')
        
        # Dependencias
        inicio >> extraer >> procesar >> cargar >> fin
    
    # CRÍTICO: Asignar cada DAG a globals() para que sea descubierto
    # globals() es un diccionario con todas las variables del módulo
    globals()[dag_id] = dag
