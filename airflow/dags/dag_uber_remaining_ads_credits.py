from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime
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
    try:
        print("Début de l'exécution...")

        print("Début du scraping...")
        df_final = run_scraping()
        print(f"Scraping terminé. Taille du DataFrame : {len(df_final)}")
        print("Création du fichier CSV...")
        output_path = "/tmp/uber_ads_remaining.csv"
        df_final.to_csv(output_path, index=False)
        print(f"Fichier CSV créé : {output_path}")

        print("Upload vers GCS...")

        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        bucket_name = 'trou-test'
        object_name = 'uber/uber_ads_remaining.csv'

        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=object_name,
            filename=output_path,
            mime_type='text/csv'
        )

        print("Upload terminé avec succès")

        os.remove(output_path)
        print(f"Les résultats ont été sauvegardés dans GCS : gs://{bucket_name}/{object_name}")

    except Exception as e:
        print(f"Erreur dans execute_scraping : {str(e)}")
        print(f"Type d'erreur : {type(e)}")
        raise

scraping_task = PythonOperator(
    task_id='execute_scraping',
    python_callable=execute_scraping,
    provide_context=True,
    dag=dag,
)

scraping_task
