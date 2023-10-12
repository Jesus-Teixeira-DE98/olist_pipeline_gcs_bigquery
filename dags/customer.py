from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta
import pandas as pd
import pyarrow

### Funções para Python Operator
def clean_data():
    data = pd.read_csv('customer_files/olist_customers_dataset.csv')
    data['customer_zip_code_prefix'] = data['customer_zip_code_prefix'].astype(int)
    data.to_parquet('customer_files/olist_customers_dataset_cleaned.parquet', compression=None, index=False)
    return None

### coletar dados da camada raw 
BUCKET_NAME_RAW = 'olist-data-project-raw'
FILE_NAME = 'olist_customers_dataset.csv'
PATH_TO_SAVED_FILE = '/home/jesus/Documentos/repos2/olist_data_project/customer_files/olist_customers_dataset.csv'

### enviar dados camada cleaned
BUCKET_NAME_CLEAN = 'olist-data-project-cleaned'
FILE_NAME_CLEANED = '/home/jesus/Documentos/repos2/olist_data_project/customer_files/olist_customers_dataset_cleaned.parquet'
DESTINATION = 'olist_customers_dataset_cleaned.parquet'

### coletar dados camada cleaned
FILE_NAME_2 = 'olist_order_reviews_dataset_cleaned.csv'
PATH_TO_SAVED_FILE_2 = '/home/jesus/Documentos/repos2/olist_data_project/order_review_files/olist_order_reviews_dataset_cleaned.csv'

## Local to trusted
BUCKET_NAME_TRUSTED = 'olist-data-project-trusted'
FILE_NAME_TRUSTED = '/home/jesus/Documentos/repos2/olist_data_project/order_review_files/olist_order_reviews_dataset_trusted.parquet'
DESTINATION_TRUSTED = 'olist_order_reviews_dataset_trusted.parquet'

default_args = {
    'owner': 'jesus teixeira',
    'start_date': days_ago(1),
    'email': ['jesusteixeira92@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

with DAG(
    'customers_gcs_to_bigquery',
    default_args=default_args,
    description='DAG responsável pela orquestração do pipeline da dimensão customers',
    schedule_interval='@daily'
    ) as dag:

    task_1 = EmptyOperator(
        task_id='inicio'
    )

    task_2 = BashOperator(
        task_id = 'cria_pasta_local',
        bash_command= 'mkdir -p ~/Documentos/repos2/olist_data_project/customer_files'
    )

    task_3 = GCSToLocalFilesystemOperator(
        task_id = 'gcs_to_local',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_RAW,
        object_name=FILE_NAME,
        filename=PATH_TO_SAVED_FILE
    )

    task_4 = PythonOperator(
        task_id = 'altera_tipos_salva_como_parquet',
        python_callable=clean_data
    )

    task_5 = LocalFilesystemToGCSOperator(
        task_id = 'dados_local_gcs_cleaned',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_CLEAN,
        src=FILE_NAME_CLEANED,
        dst=DESTINATION
    )

    task_6 = GCSToGCSOperator(
        task_id='transfer_clean_data_to_trusted',
        source_bucket='olist-data-project-cleaned',
        source_object='olist-data-project-cleaned/olist_customers_dataset_cleaned.parquet',
        destination_bucket='olist-data-project-trusted',
        move_object=True,
        gcp_conn_id='google_cloud_default'
    )

    task_7 = BashOperator(
        task_id = 'limpa_pasta_local',
        bash_command= 'rm /home/jesus/Documentos/repos2/olist_data_project/customer_files/*.parquet'
    )

    task_8 = EmptyOperator(
        task_id='fim'
    )

task_1 >>  task_2 >> task_3 >> task_4 >> task_5 >> [task_6 , task_7] >> task_8