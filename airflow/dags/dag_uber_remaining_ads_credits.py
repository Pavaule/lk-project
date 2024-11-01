from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# Importer votre fonction de scraping
from scripts_scraping.uber_remaining_ads_credits import run_scraping

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'uber_remaining_ads_credits_dag',
    default_args=default_args,
    description='DAG pour exécuter un script de scraping Uber Ads chaque semaine et sauvegarder dans GCP',
    schedule_interval='0 2 * * 1',  # Tous les lundis à 2h du matin
    start_date=days_ago(1),
    catchup=False,
)

def execute_scraping(**kwargs):
    df_final = run_scraping()
    output_path = "/tmp/uber_ads_results.csv"
    df_final.to_csv(output_path, index=False)

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    bucket_name = 'airflow-results-bucket'
    object_name = 'uber_ads_results/uber_ads_results.csv'

    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        filename=output_path,
        mime_type='text/csv'
    )

    os.remove(output_path)
    print(f"Les résultats ont été sauvegardés dans GCS : gs://{bucket_name}/{object_name}")

scraping_task = PythonOperator(
    task_id='execute_scraping',
    python_callable=execute_scraping,
    provide_context=True,
    dag=dag,
)

scraping_task
