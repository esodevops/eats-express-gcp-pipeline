from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from google.cloud.storage import Client


default_args = {
    'owner': 'airflow',
    #'email': 'duniola4u@gmail.com',
    #'email_on_failure': True,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(seconds=60)
}

def check_file_existence(bucket_name, prefix, processed_prefix='processed/'):
    gcs_hook = GCSHook()
    files = gcs_hook.list(bucket_name, prefix=prefix)
    if files:
        source_object = files[0]
        file_name = source_object.split("/")[-1]
        destination_object = processed_prefix.rstrip('/') + '/' + file_name

        storage_client = Client()  
        bucket = storage_client.get_bucket(bucket_name)  
        source_blob = bucket.blob(source_object)

        destination_blob = bucket.blob(destination_object)
        destination_blob.upload_from_string(source_blob.download_as_text())

        # Delete the source_blob
        source_blob.delete()

        return destination_object
    else:
        return None


with DAG(
    'food_orders_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1) as dag:

    gcs_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id='gcs_sensor',
        bucket='eats-express-bkt',
        prefix='food_daily',
        mode='poke',
        poke_interval=60,
        timeout=300
    )

    check_file = PythonOperator(
        task_id='check_file',
        python_callable=check_file_existence,
        op_kwargs={
            'bucket_name': 'eats-express-bkt',
            'prefix': 'food_daily'
        },
        do_xcom_push=True
    )
    beam_task = BeamRunPythonPipelineOperator(
        task_id='beam_task',
        runner='DataflowRunner',
        py_file='gs://eats-express-bkt/beam.py',
        pipeline_options={
            'input': "gs://eats-express-bkt/{{task_instance.xcom_pull('check_file')}}"
        },
        py_options=[],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config=DataflowConfiguration(
            job_name='food-orders-pipeline',
            project_id='eats-express-512014',
            location='us-east1'
        )
    )

    gcs_sensor >> check_file >> beam_task


