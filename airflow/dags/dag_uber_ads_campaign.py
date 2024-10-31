from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Fonction Python qui sera exécutée par le PythonOperator
def hello_world():
    print("Hello World from Airflow!")

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 30),  # Assurez-vous que la date est dans le passé pour que le DAG s'exécute immédiatement
    'retries': 1,
}

with DAG(
    dag_id='uber_ads_campaign_hello_world',  # Nom unique pour le DAG
    default_args=default_args,
    description='Un DAG de base pour tester Airflow',
    schedule_interval='@daily',  # Le DAG s'exécute tous les jours
    catchup=False,  # Pour éviter d'exécuter les dates manquantes si vous démarrez en retard
) as dag:

    # Déclaration de la tâche utilisant PythonOperator
    hello_task = PythonOperator(
        task_id='print_hello_world',
        python_callable=hello_world
    )

    # Ici, nous pouvons définir des dépendances entre les tâches s'il y en a plusieurs
    # Pour ce DAG de base, nous n'avons qu'une seule tâche
    hello_task
