"""
AIRFLOW DAG - Reddit Data Pipeline Orchestration
Architecture: Reddit API → Kafka → Spark → PostgreSQL

Pipeline Steps:
1. Fetch data from Reddit API (simulated with JSONL)
2. Produce messages to Kafka topic
3. Spark Structured Streaming consumes and processes
4. Write processed data to PostgreSQL
5. Run analytics queries
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import json
import logging
import os
logger = logging.getLogger(__name__)


# ============================================================================
# Default Arguments
# ============================================================================
# Default arguments for the DAG
default_args = {
    'owner': 'phongtd11',
    'start_date': datetime(2025, 3, 12),
    'retries': 1,
}

# ============================================================================
# DAG Definition
# ============================================================================
with DAG(
    dag_id='spark-submit-only',
    default_args=default_args,
    description='',
    schedule_interval=None,  # Every 4 hours
    catchup=False,
    max_active_runs=1,
    tags=['test']
) as dag:
    
    spark_submit = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/spark_scripts/spark_pipeline_full.py",
        conn_id="spark_standalone",
        name='arrow-spark',
        deploy_mode='client',
        # XÓA dòng packages, thay bằng jars
        jars='/opt/spark-jars/kafka/spark-sql-kafka-0-10_2.12-3.5.1.jar,'
            '/opt/spark-jars/kafka/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,'
            '/opt/spark-jars/kafka/kafka-clients-3.4.1.jar,'
            '/opt/spark-jars/kafka/commons-pool2-2.11.1.jar',
        verbose=True
    )


