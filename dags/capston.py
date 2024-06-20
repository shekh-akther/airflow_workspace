from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
from airflow.exceptions import AirflowSkipException
import pandas as pd
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import logging



GCP_PROJECT = 'aflow-training-eneco-20240605'

BUCKET_NAME = 'aflow-training-eneco-20240605'
DEST_FILE_NAME = 'shekh/launch.parquet'
DATASET_NAME = 'aflow-training-eneco-20240605.aflow_training_eneco_20240605'
TABLE_NAME = 'shekh_'



def _parse_response(task_instance, **_):
    response =  json.loads(task_instance.xcom_pull(task_ids="api_get_launchs"))
    print(response)
    if response['count'] > 0:
        print('we have launch data!!!')
    else:
        raise AirflowSkipException('No launch record found')   


def _write_parquet_file(task_instance, ds, **_):
    df = pd.read_json(task_instance.xcom_pull(task_ids="api_get_launchs"))
    df_results = df['results']

    data=[]    

    for result in df_results:
        print(f"rocket id: {result['rocket']['id']}")
        print(f"rocket name: {result['rocket']['configuration']['name']}")
        print(f"mission name: {result['mission']['name']}")
        data.append(
            {
                'rocket_id': {result['rocket']['id']}, 
                'rocket_name': {result['rocket']['configuration']['name']},
                'mission_name': {result['mission']['name']}
            }
        )

    df = pd.DataFrame(data)     
    print(df)
    SRC_FILE_NAME = f"/tmp/launch_{ds}.parquet"
    DEST_FILE_NAME = f"shekh/launch_{ds}.parquet"
    df.to_parquet(SRC_FILE_NAME)


with DAG(
    dag_id="capston_dag", 
    start_date=datetime(2024, 5, 28),
    schedule="@daily"
):
    task_is_api_alive = HttpSensor(
        task_id="is_api_alive",
        method='GET',
        http_conn_id='thespacedevs_api',
        endpoint='/launch/',
        headers={'Content-Type': 'application/json'},
        timeout=60
    )
    
    task_api_get_launchs = SimpleHttpOperator(
        task_id='api_get_launchs',
        method='GET',
        http_conn_id='thespacedevs_api',
        endpoint='/launch/',
        data={'window_start__gte': "{{data_interval_start|ts}}" , 'window_end__lt':  "{{data_interval_end|ts}}"  },
        headers={'Content-Type': 'application/json'},
        log_response=True
    )


    task_is_roket_launch = PythonOperator(task_id='is_roket_launch', python_callable=_parse_response )

    task_write_parquet_file =  PythonOperator(task_id='write_parquet_file', python_callable=_write_parquet_file)

    task_parquet_into_gcs = LocalFilesystemToGCSOperator(
        task_id='parquet_into_gcs', 
        gcp_conn_id='google_cloud_con',
        src="/tmp/launch_{{ds}}.parquet", dst=DEST_FILE_NAME, bucket=BUCKET_NAME,)
    
    task_gcs_to_bigquery = GCSToBigQueryOperator(task_id='gcs_to_bigquery', gcp_conn_id='google_cloud_con', 
                                                 bucket=BUCKET_NAME,source_objects=[DEST_FILE_NAME],
                                                #  schema_fields=['rocket_id', 'rocket_name', 'mission_name'],
                                                 autodetect=True,
                                                 source_format='PARQUET',
                                                 write_disposition='WRITE_APPEND',
                                                 destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}"+ "{{ds}}")


    task_is_api_alive >> task_api_get_launchs >> task_is_roket_launch >> task_write_parquet_file >> task_parquet_into_gcs >> task_gcs_to_bigquery