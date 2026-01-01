from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

import pymysql

# default arguments for DAG
default_args = {
    "owner": "owner_name",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10)
}

# Validate ETL success from data written to database
def validate_sales_data():
    # Create database connection
    connection = pymysql.connect(
        host="host.docker.internal",
        user="mysql_user_name",
        password="user_password",
        database="database_name"
    )

    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM customers_data_mart")
    row_count = cursor.fetchone()[0]

    if row_count == 0:
        raise AirflowFailException("customers_data_mart table is empty")

    cursor.close()
    connection.close()

# Define DAG attributes and tasks
with DAG(
    dag_id="retail_sales_batch_etl_local",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    
    # s3 sensor task: ensures ETL runs only when data is available
    wait_for_sales_data = S3KeySensor(
        task_id="wait_for_sales_data",
        bucket_name="s3_bucket_name",
        bucket_key="sales_data/*.csv",
        wildcard_match=True,
        aws_conn_id="connection_id_name",
        poke_interval=180,
        timeout=1800,
    )

    # Create spark-submit for ETL task
    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
        export PYTHONPATH=filepath to folder containing main.py and dependencies:$PYTHONPATH
        
        spark-submit \
        --master local[*] \
        /mnt/c/filepath/to/main.py
        """
    )

    # Validate ETL success
    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=validate_sales_data
    )

    # DAG flow
    wait_for_sales_data >> run_spark_etl >> validate_data


