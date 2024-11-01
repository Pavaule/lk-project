from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 30),  # Assurez-vous que la date est dans le passé
    'retries': 1,
}

with DAG(
    dag_id='gcs_list_buckets_test',
    default_args=default_args,
    schedule_interval=None,  # Exécution manuelle
    catchup=False,
) as dag:

    list_gcs_buckets = GCSListObjectsOperator(
        task_id='list_buckets',
        bucket='airflow-results-bucket',  # Remplacez par le nom de votre bucket GCS
        gcp_conn_id='google_cloud_default'
    )

    list_gcs_buckets
