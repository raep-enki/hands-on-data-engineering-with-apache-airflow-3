"""
DAG Packaging - Template Pattern con Herencia

Demuestra cómo usar clases base y herencia para estandarizar
la estructura de DAGs, útil para equipos grandes con convenciones
estrictas de desarrollo.
"""

import datetime
from typing import List, Dict, Any

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# Clase base para estandarizar DAGs (estaría en dags/templates/base_dag.py)
class StandardETLDAG:
    """
    Template base para DAGs ETL estandarizados.
    Define la estructura común que todos los DAGs ETL deben seguir.
    """
    
    def __init__(self, dag_id: str, source: str, destination: str, schedule: str):
        self.dag_id = dag_id
        self.source = source
        self.destination = destination
        self.schedule = schedule
        self.default_args = self._get_default_args()
        self.tags = self._get_tags()
    
    def _get_default_args(self) -> Dict[str, Any]:
        """Define default_args estándar para todos los DAGs ETL"""
        return {
            'owner': 'data_engineering',
            'retries': 2,
            'retry_delay': datetime.timedelta(minutes=5)
        }
    
    def _get_tags(self) -> List[str]:
        """Define tags estándar"""
        return [
            'example', 'packaging_dags',
            'etl', 'template', self.source.lower()
        ]
    
    def create_dag(self) -> DAG:
        """Crea el DAG con la estructura estándar"""
        with DAG(
            dag_id=self.dag_id,
            schedule=self.schedule,
            start_date=datetime.datetime(2021, 1, 1),
            catchup=False,
            default_args=self.default_args,
            tags=self.tags
        ) as dag:
            
            start = EmptyOperator(task_id='start')
            
            # Pre-processing hook (puede ser sobrescrito)
            pre_process = self._create_pre_process_task()
            
            # ETL tasks estándar
            extract = self._create_extract_task()
            validate = self._create_validate_task()
            transform = self._create_transform_task()
            load = self._create_load_task()
            
            # Post-processing hook (puede ser sobrescrito)
            post_process = self._create_post_process_task()
            
            end = EmptyOperator(task_id='end')
            
            # Flujo estándar
            start >> pre_process >> extract >> validate >> transform >> load >> post_process >> end
        
        return dag
    
    def _create_pre_process_task(self):
        """Hook para pre-procesamiento (puede ser sobrescrito por subclases)"""
        return BashOperator(
            task_id='pre_process',
            bash_command=f'echo "🔧 Preparando entorno para {self.source}"'
        )
    
    def _create_extract_task(self):
        """Crea tarea de extracción"""
        return BashOperator(
            task_id='extract',
            bash_command=f'echo "📥 Extrayendo datos de {self.source}"'
        )
    
    def _create_validate_task(self):
        """Crea tarea de validación"""
        return BashOperator(
            task_id='validate',
            bash_command=f'echo "✅ Validando datos de {self.source}"'
        )
    
    def _create_transform_task(self):
        """Crea tarea de transformación"""
        return BashOperator(
            task_id='transform',
            bash_command=f'echo "⚙️ Transformando datos"'
        )
    
    def _create_load_task(self):
        """Crea tarea de carga"""
        return BashOperator(
            task_id='load',
            bash_command=f'echo "📤 Cargando a {self.destination}"'
        )
    
    def _create_post_process_task(self):
        """Hook para post-procesamiento (puede ser sobrescrito por subclases)"""
        return BashOperator(
            task_id='post_process',
            bash_command='echo "✨ Post-procesamiento completado"'
        )


# Crear DAGs usando el template
api_etl = StandardETLDAG(
    dag_id='packaging_template_api_etl',
    source='REST_API',
    destination='data_warehouse.api_data',
    schedule='@hourly'
).create_dag()

database_etl = StandardETLDAG(
    dag_id='packaging_template_database_etl',
    source='PostgreSQL',
    destination='data_warehouse.db_data',
    schedule='@daily'
).create_dag()

files_etl = StandardETLDAG(
    dag_id='packaging_template_files_etl',
    source='S3_Files',
    destination='data_warehouse.file_data',
    schedule='0 */6 * * *'  # cada 6 horas
).create_dag()
