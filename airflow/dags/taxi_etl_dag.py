from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

# DEFINE CONFIGS
# We read these from the Airflow container's environment variables
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')

# Spark Configs (identical to what we used in the scripts)
SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint": f"http://{MINIO_ENDPOINT}",
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    'nyc_taxi_etl_2016',
    default_args=default_args,
    description='Process 12 months of NYC Taxi Data',
    schedule_interval=None, # Trigger manually
    catchup=False,
    max_active_runs=1
) as dag:

    # LOOP THROUGH MONTHS 1 TO 12
    for i in range(1, 13):
        # Format month as 2016-01, 2016-02, etc.
        year_month = f"2016-{i:02d}"
        
        # TASK 1: DOWNLOAD (BashOperator)
        # We use Bash to call the python script directly
        download_task = BashOperator(
            task_id=f'download_{year_month}',
            bash_command=f'python /opt/airflow/scripts/download_taxi.py {year_month}'
        )

        # TASK 2: TRANSFORM (SparkSubmitOperator)
        transform_task = SparkSubmitOperator(
            task_id=f'transform_{year_month}',
            application='/opt/spark-jobs/transform_taxi.py',
            application_args=[year_month],
            conn_id='spark_default',
            conf=SPARK_CONF,
            verbose=True
        )

        # TASK 3: AGGREGATE (SparkSubmitOperator)
        aggregate_task = SparkSubmitOperator(
            task_id=f'aggregate_{year_month}',
            application='/opt/spark-jobs/aggregate_zones.py',
            application_args=[year_month],
            conn_id='spark_default',
            conf=SPARK_CONF,
            verbose=True
        )

        # TASK 4: LOAD TO POSTGRES (BashOperator)
        load_task = BashOperator(
            task_id=f'load_{year_month}',
            bash_command=f'python /opt/airflow/scripts/load_to_postgres.py {year_month}'
        )

        # DEFINE DEPENDENCIES
        # download -> transform -> aggregate -> load
        download_task >> transform_task >> aggregate_task >> load_task