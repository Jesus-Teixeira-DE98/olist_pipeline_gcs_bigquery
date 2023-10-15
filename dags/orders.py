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
    data = pd.read_csv('orders_files/olist_order_items_dataset.csv')
    data1 = pd.read_csv('orders_files/olist_orders_dataset.csv')
    order_base = data1.merge( data, how='left', on='order_id' , suffixes=(False, False))
    order_base['order_purchase_timestamp'] = pd.to_datetime(order_base['order_purchase_timestamp'])
    order_base['order_approved_at'] = pd.to_datetime(order_base['order_approved_at'])
    order_base['order_delivered_carrier_date'] = pd.to_datetime(order_base['order_delivered_carrier_date'])
    order_base['order_delivered_customer_date'] = pd.to_datetime(order_base['order_delivered_customer_date'])
    order_base['order_estimated_delivery_date'] = pd.to_datetime(order_base['order_estimated_delivery_date'])
    order_base['shipping_limit_date'] = pd.to_datetime(order_base['shipping_limit_date'])
    order_base['check_null'] = order_base['order_item_id'].apply(lambda x: pd.isna(x))
    order_base = order_base[order_base['check_null'] == False]
    order_base.drop('check_null', axis=1, inplace=True)
    order_base.reset_index(drop=True, inplace=True)
    order_base.to_parquet('orders_files/olist_orders_dataset_cleaned.parquet', compression=None, index=False)
    return None

### coletar dados da camada raw 
BUCKET_NAME_RAW = 'olist-data-project-raw'
FILE_NAME_1= 'olist_order_items_dataset.csv'
FILE_NAME_2 = 'olist_orders_dataset.csv'
PATH_TO_SAVED_FILE_1 = '/home/jesus/Documentos/repos2/olist_data_project/orders_files/olist_order_items_dataset.csv'
PATH_TO_SAVED_FILE_2 = '/home/jesus/Documentos/repos2/olist_data_project/orders_files/olist_orders_dataset.csv'

### enviar dados camada cleaned
BUCKET_NAME_CLEAN = 'olist-data-project-cleaned'
FILE_NAME_CLEANED = '/home/jesus/Documentos/repos2/olist_data_project/orders_files/olist_orders_dataset_cleaned.parquet'
DESTINATION = 'olist_orders_dataset_cleaned.parquet'

# extração camada cleaned 
#FILE_NAME_CLEANED_EXT = 'olist_sellers_dataset_cleaned.parquet'
#PATH_TO_SAVED_FILE_3 = '/home/jesus/Documentos/repos2/olist_data_project/orders_files/olist_sellers_dataset_cleaned.parquet'

## Local to trusted
BUCKET_NAME_TRUSTED = 'olist-data-project-trusted'
FILE_NAME_TRUSTED = '/home/jesus/Documentos/repos2/olist_data_project/orders_files/olist_orders_dataset_trusted.parquet'
DESTINATION_TRUSTED = 'olist_orders_dataset_trusted.parquet'

### DAG
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
    'orders_gcs_to_bigquery',
    default_args=default_args,
    description='DAG responsável pela orquestração do pipeline da fato orders',
    schedule_interval='@daily'
    ) as dag:

    task_1 = EmptyOperator(
        task_id='inicio'
    )

    task_2 = BashOperator(
        task_id='cria_pasta_local',
        bash_command='mkdir -p ~/Documentos/repos2/olist_data_project/orders_files'
    )

    task_3 = GCSToLocalFilesystemOperator(
        task_id='extrai_dados_raw_gcs_orders',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_RAW,
        object_name=FILE_NAME_1,
        filename=PATH_TO_SAVED_FILE_1
    )

    task_4 = GCSToLocalFilesystemOperator(
        task_id='extrai_dados_raw_gcs_orders_items',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_RAW,
        object_name=FILE_NAME_2,
        filename=PATH_TO_SAVED_FILE_2
    )

    task_5 = PythonOperator(
        task_id='limpa_dados_salva_parquet',
        python_callable=clean_data
    )

    task_6 = BashOperator(
        task_id='copia_renomeia_cleaned_trusted',
        bash_command='cp /home/jesus/Documentos/repos2/olist_data_project/orders_files/olist_orders_dataset_cleaned.parquet /home/jesus/Documentos/repos2/olist_data_project/orders_files/olist_orders_dataset_trusted.parquet '
    )

    task_7 = LocalFilesystemToGCSOperator(
        task_id = 'dados_local_gcs_cleaned',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_CLEAN,
        src=FILE_NAME_CLEANED,
        dst=DESTINATION
    )

    task_8 = LocalFilesystemToGCSOperator(
        task_id = 'dados_local_gcs_trusted',
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME_TRUSTED,
        src=FILE_NAME_TRUSTED,
        dst=DESTINATION_TRUSTED
    )

    task_9 = EmptyOperator(
        task_id='junção'
    )

    task_10 = BashOperator(
        task_id = 'limpa_pasta_local_parquet',
        bash_command= 'rm /home/jesus/Documentos/repos2/olist_data_project/orders_files/*.parquet'
    )

    task_11 = BashOperator(
        task_id = 'limpa_pasta_local_csv',
        bash_command= 'rm /home/jesus/Documentos/repos2/olist_data_project/orders_files/*.csv'
    )  

    task_12 = EmptyOperator(
        task_id='fim'
    )

task_1 >> task_2 >> [task_3, task_4] >> task_5 >> task_6 >> [task_7, task_8] >> task_9 >> [task_10, task_11] >> task_12