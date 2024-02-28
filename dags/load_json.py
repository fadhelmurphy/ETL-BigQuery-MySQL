from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.oauth2 import service_account
import os

# Function to load credentials
def load_credentials():
    # Mendapatkan path ke direktori dags
    dags_dir = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(dags_dir, "auth/credentials.json")

    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        print("Credentials loaded successfully.")
        return credentials
    except Exception as e:
        print(f"Error loading credentials: {e}")
        return None

# Initialize DAG
dag = DAG(
    'load_credentials_dag',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
)

# Python Operator to load credentials
load_credentials_task = PythonOperator(
    task_id='load_credentials_task',
    python_callable=load_credentials,
    dag=dag,
)

# Add dependencies if needed
# load_credentials_task.set_upstream(...)

if __name__ == "__main__":
    dag.cli()
