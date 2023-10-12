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

### Python Functions
def clean_data():
    data = pd.read_csv('payment_files/olist_order_payments_dataset.csv')
    ## número de parcelas igual a 0 não deve existir, lançamento errado. substitindo por 1
    data['payment_installments'] = data['payment_installments'].apply(lambda x: 1 if x == 0 else x)
    data.to_parquet('payment_files/olist_order_payments_dataset_cleaned.parquet', compression=None, index=False)
    return None

def trusted_data():
    def parcelamento_taxas(row):
        if row['payment_type'] == 'credit_card':
            if row['payment_installments'] == 1:
                return 0.04 * row['payment_value']
            else:
                return 0.09 * row['payment_value']
        elif row['payment_type'] == 'debit_card':
            return 0.0149 * row['payment_value']
        else:
            return 0

    def voucher_high_value(row):
        if (row['payment_type'] == 'voucher') and row['payment_value'] > 100:
            return 'validar_promo'
        else:
            return 'ok'
        
    data = pd.read_parquet('payment_files/olist_order_payments_dataset_cleaned.parquet')
    data['payment_method_flag'] = data['payment_sequential'].apply(lambda x: 1 if x < 4 else 2 if x < 11 else 3)
    data['installments_fees'] = data.apply(parcelamento_taxas, axis=1)
    data['check_voucher'] = data.apply(voucher_high_value, axis=1)
    data.to_parquet('payment_files/olist_order_payments_dataset_trusted.parquet', compression=None, index=False)
    return None

### coletar dados da camada raw 
BUCKET_NAME_RAW = 'olist-data-project-raw'
FILE_NAME = 'olist_order_payments_dataset.csv'
PATH_TO_SAVED_FILE = '/home/jesus/Documentos/repos2/olist_data_project/payment_files/olist_order_payments_dataset.csv'

### enviar dados camada cleaned
BUCKET_NAME_CLEAN = 'olist-data-project-cleaned'
FILE_NAME_CLEANED = '/home/jesus/Documentos/repos2/olist_data_project/payment_files/olist_order_payments_dataset_cleaned.parquet'
DESTINATION = 'olist_order_payments_dataset_cleaned.parquet'

# extração camada cleaned 
FILE_NAME_CLEANED_EXT = 'olist_order_payments_dataset_cleaned.parquet'
PATH_TO_SAVED_FILE_2 = '/home/jesus/Documentos/repos2/olist_data_project/payment_files/olist_order_payments_dataset_cleaned.parquet'

## Local to trusted
BUCKET_NAME_TRUSTED = 'olist-data-project-trusted'
FILE_NAME_TRUSTED = '/home/jesus/Documentos/repos2/olist_data_project/payment_files/olist_order_payments_dataset_trusted.parquet'
DESTINATION_TRUSTED = 'olist_order_payments_dataset_trusted.parquet'

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
    'payments_gcs_to_bigquery',
    default_args=default_args,
    description='DAG responsável pela orquestração do pipeline da dimensão payments',
    schedule_interval='@daily'
    ) as dag:

    task_1 = EmptyOperator(
        task_id='inicio'
    )

    task_2 = BashOperator(
        task_id='cria_pasta_local',
        bash_command='mkdir -p ~/Documentos/repos2/olist_data_project/payment_files'
    )
    
    task_3 = GCSToLocalFilesystemOperator(
        task_id='extrai_dados_raw_gcs',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_RAW,
        object_name=FILE_NAME,
        filename=PATH_TO_SAVED_FILE
    )

    task_4 = PythonOperator(
        task_id = 'limpa_salva_parquet',
        python_callable=clean_data
    )

    task_5 = LocalFilesystemToGCSOperator(
        task_id = 'dados_local_gcs_cleaned',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_CLEAN,
        src=FILE_NAME_CLEANED,
        dst=DESTINATION
    )

    task_6 = BashOperator(
        task_id = 'limpa_pasta_local_parquet',
        bash_command= 'rm /home/jesus/Documentos/repos2/olist_data_project/payment_files/*.parquet'
    )

    task_7 = BashOperator(
        task_id = 'limpa_pasta_local_csv',
        bash_command= 'rm /home/jesus/Documentos/repos2/olist_data_project/payment_files/*.csv'
    )


    task_8 = GCSToLocalFilesystemOperator(
        task_id='extrai_dados_cleaned_gcs',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_CLEAN,
        object_name=FILE_NAME_CLEANED_EXT,
        filename=PATH_TO_SAVED_FILE_2
    )

    task_9 = PythonOperator(
        task_id = 'aplica_regra_negocio_trusted_data',
        python_callable=trusted_data
    )

    task_10 = LocalFilesystemToGCSOperator(
        task_id = 'dados_local_gcs_trusted',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_TRUSTED,
        src=FILE_NAME_TRUSTED,
        dst=DESTINATION_TRUSTED
    )

    task_11 = BashOperator(
        task_id = 'limpa_pasta_local_parquet_trusted',
        bash_command= 'rm /home/jesus/Documentos/repos2/olist_data_project/payment_files/*.parquet'
    )

    task_12 = EmptyOperator(
        task_id='fim'
    )

task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> [ task_6, task_7] >> task_8 >> task_9 >> [task_10, task_11] >> task_12