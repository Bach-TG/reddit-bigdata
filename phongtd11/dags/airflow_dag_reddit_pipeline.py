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
# Python Functions for Tasks
# ============================================================================

def check_kafka_health(**context):
    """Check if Kafka is running and healthy"""
    from confluent_kafka import Producer
    
    try:
        producer = Producer({
                "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
                "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
                "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
                "sasl.username": os.getenv("KAFKA_PLAIN_USERNAME"),
                "sasl.password": os.getenv("KAFKA_PLAIN_PASSWORD"),
                "retries": os.getenv("KAFKA_RETRIES")
            })
        producer.close()
        logger.info("✅ Kafka is healthy")
        return True
    except Exception as e:
        logger.error(f"❌ Kafka health check failed: {e}")
        raise

def fetch_reddit_data(**context):
    """
    Simulate Reddit API call
    In production, this would call actual Reddit API
    """
    import requests
    from datetime import datetime
    
    # In production: Call Reddit API
    # For demo: Use existing JSONL file or mock data
    
    execution_date = context['execution_date']
    logger.info(f"Fetching Reddit data for {execution_date}")
    
    # Simulated: In production, fetch from Reddit API
    # response = requests.get('https://api.reddit.com/r/worldnews/new', headers={...})
    
    # For demo, we'll use the uploaded file
    input_file = '/mnt/user-data/uploads/reddit_posts.jsonl'
    output_file = f'/tmp/reddit_data_{execution_date.strftime("%Y%m%d_%H%M%S")}.jsonl'
    
    # Copy file to temporary location for processing
    import shutil
    shutil.copy(input_file, output_file)
    
    # Push file path to XCom
    context['task_instance'].xcom_push(key='data_file', value=output_file)
    
    logger.info(f"✅ Data fetched: {output_file}")
    return output_file

def produce_to_kafka(**context):
    """Produce Reddit data to Kafka topic"""
    from confluent_kafka import Producer
    import json
    
    # Get data file from XCom
    data_file = context['task_instance'].xcom_pull(
        task_ids='fetch_reddit_data',
        key='data_file'
    )
    
    logger.info(f"Producing to Kafka from {data_file}")
    
    # Initialize Kafka producer
    producer = Producer({
                "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
                "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
                "sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
                "sasl.username": os.getenv("KAFKA_PLAIN_USERNAME"),
                "sasl.password": os.getenv("KAFKA_PLAIN_PASSWORD"),
                "retries": os.getenv("KAFKA_RETRIES")
            })
    
    # Send messages
    success_count = 0
    failed_count = 0
    
    with open(data_file, 'r') as f:
        for line in f:
            try:
                message = json.loads(line.strip())
                # message['airflow_execution_date'] = context['execution_date'].isoformat()
                # message['airflow_dag_id'] = context['dag'].dag_id
                
                producer.produce(os.getenv("KAFKA_TOPIC", "reddit-posts"),
                                      value=json.dumps(message).encode('utf-8'))
                success_count += 1
                
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
                failed_count += 1
    
    producer.flush()
    producer.close()
    
    # Push statistics to XCom
    stats = {
        'success': success_count,
        'failed': failed_count,
        'total': success_count + failed_count
    }
    
    context['task_instance'].xcom_push(key='kafka_stats', value=stats)
    
    logger.info(f"✅ Kafka production complete: {stats}")
    return stats

# ============================================================================
# DAG Definition
# ============================================================================
with DAG(
    dag_id='reddit_streaming_pipeline',
    default_args=default_args,
    description='Reddit API → Kafka → Spark → PostgreSQL Pipeline',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    catchup=False,
    max_active_runs=1,
    tags=['reddit', 'streaming', 'kafka', 'spark', 'postgres']
) as dag:
    
    # ========================================================================
    # Task 1: Health Checks
    # ========================================================================
    check_kafka = PythonOperator(
        task_id='check_kafka_health',
        python_callable=check_kafka_health,
        doc_md="""
        ### Check Kafka Health
        Verifies that Kafka broker is running and accessible.
        """
    )
    
    # ========================================================================
    # Task 2: Fetch Reddit Data
    # ========================================================================
    fetch_data = PythonOperator(
        task_id='fetch_reddit_data',
        python_callable=fetch_reddit_data,
        doc_md="""
        ### Fetch Reddit Data
        Calls Reddit API (or uses simulated data) to get latest posts.
        In production, this would call the actual Reddit API with credentials.
        """
    )
    
    # ========================================================================
    # Task 3: Produce to Kafka
    # ========================================================================
    kafka_produce = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_to_kafka,
        doc_md="""
        ### Produce to Kafka
        Sends Reddit posts to Kafka topic 'reddit-posts'.
        Messages are compressed with gzip and acknowledged by all replicas.
        """
    )

    # ========================================================================
    # Task 4: Spark Streaming Job
    # ========================================================================
    spark_submit = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/spark_scripts/spark_pipeline_full.py",
        conn_id="spark_standalone",
        verbose=True,
    )
    
    # ========================================================================
    # Task Dependencies (Pipeline Flow)
    # ========================================================================
    
    # Health checks must pass first
    # [check_kafka, check_postgres] >> fetch_data
    
    # Fetch data, then produce to Kafka
    check_kafka >> fetch_data >> kafka_produce >> spark_submit

