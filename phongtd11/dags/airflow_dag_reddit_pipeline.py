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
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# Default Arguments
# ============================================================================
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}


# ============================================================================
# Python Functions for Tasks
# ============================================================================

def check_kafka_health(**context):
    """Check if Kafka is running and healthy"""
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            request_timeout_ms=5000
        )
        producer.close()
        logger.info("✅ Kafka is healthy")
        return True
    except Exception as e:
        logger.error(f"❌ Kafka health check failed: {e}")
        raise


def check_postgres_health(**context):
    """Check if PostgreSQL is running and accessible"""
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_reddit')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        connection.close()
        logger.info("✅ PostgreSQL is healthy")
        return True
    except Exception as e:
        logger.error(f"❌ PostgreSQL health check failed: {e}")
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
    from kafka import KafkaProducer
    import json
    
    # Get data file from XCom
    data_file = context['task_instance'].xcom_pull(
        task_ids='fetch_reddit_data',
        key='data_file'
    )
    
    logger.info(f"Producing to Kafka from {data_file}")
    
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        compression_type='gzip',
        acks='all'
    )
    
    # Send messages
    success_count = 0
    failed_count = 0
    
    with open(data_file, 'r') as f:
        for line in f:
            try:
                message = json.loads(line.strip())
                message['airflow_execution_date'] = context['execution_date'].isoformat()
                message['airflow_dag_id'] = context['dag'].dag_id
                
                future = producer.send('reddit-posts', value=message)
                future.get(timeout=10)  # Wait for acknowledgment
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


def validate_postgres_data(**context):
    """Validate that data was written to PostgreSQL"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_reddit')
    
    # Check record count
    sql = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT subreddit) as subreddits,
        AVG(score) as avg_score,
        MIN(created_datetime) as earliest_post,
        MAX(created_datetime) as latest_post
    FROM reddit_posts
    WHERE processing_timestamp >= NOW() - INTERVAL '1 hour'
    """
    
    result = pg_hook.get_first(sql)
    
    validation = {
        'total_records': result[0],
        'subreddits': result[1],
        'avg_score': float(result[2]) if result[2] else 0,
        'earliest_post': str(result[3]),
        'latest_post': str(result[4])
    }
    
    logger.info(f"✅ PostgreSQL validation: {validation}")
    
    # Check if we got data
    if validation['total_records'] == 0:
        raise ValueError("No records found in PostgreSQL from recent processing")
    
    context['task_instance'].xcom_push(key='validation', value=validation)
    return validation


def generate_data_quality_report(**context):
    """Generate data quality metrics"""
    pg_hook = PostgresHook(postgres_conn_id='postgres_reddit')
    
    # Quality metrics
    sql = """
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN title_cleaned IS NULL THEN 1 END) as null_titles,
        COUNT(CASE WHEN sentiment IS NULL THEN 1 END) as null_sentiment,
        COUNT(CASE WHEN topic IS NULL THEN 1 END) as null_topic,
        AVG(title_word_count) as avg_title_length,
        COUNT(DISTINCT author) as unique_authors
    FROM reddit_posts
    WHERE processing_timestamp >= NOW() - INTERVAL '1 hour'
    """
    
    result = pg_hook.get_first(sql)
    
    report = {
        'total_records': result[0],
        'data_quality': {
            'null_titles': result[1],
            'null_sentiment': result[2],
            'null_topic': result[3]
        },
        'metrics': {
            'avg_title_length': float(result[4]) if result[4] else 0,
            'unique_authors': result[5]
        }
    }
    
    # Calculate quality score
    quality_score = 100 - (
        (report['data_quality']['null_titles'] / report['total_records'] * 100) +
        (report['data_quality']['null_sentiment'] / report['total_records'] * 100) +
        (report['data_quality']['null_topic'] / report['total_records'] * 100)
    )
    
    report['quality_score'] = round(quality_score, 2)
    
    logger.info(f"✅ Data Quality Report: {report}")
    context['task_instance'].xcom_push(key='quality_report', value=report)
    
    return report


def send_success_notification(**context):
    """Send success notification (email, Slack, etc.)"""
    # Get statistics from previous tasks
    kafka_stats = context['task_instance'].xcom_pull(
        task_ids='produce_to_kafka',
        key='kafka_stats'
    )
    
    validation = context['task_instance'].xcom_pull(
        task_ids='validate_postgres_data',
        key='validation'
    )
    
    quality_report = context['task_instance'].xcom_pull(
        task_ids='generate_quality_report',
        key='quality_report'
    )
    
    message = f"""
    ✅ Reddit Data Pipeline - SUCCESS
    
    Execution Date: {context['execution_date']}
    DAG: {context['dag'].dag_id}
    
    📊 Kafka Stats:
       - Messages Sent: {kafka_stats['success']}
       - Failed: {kafka_stats['failed']}
    
    📊 PostgreSQL:
       - Records Written: {validation['total_records']}
       - Subreddits: {validation['subreddits']}
       - Avg Score: {validation['avg_score']:.2f}
    
    📊 Data Quality:
       - Quality Score: {quality_report['quality_score']}%
       - Unique Authors: {quality_report['metrics']['unique_authors']}
    
    ✅ Pipeline completed successfully!
    """
    
    logger.info(message)
    
    # In production: Send to Slack, email, etc.
    # slack_webhook = SlackWebhookOperator(...)
    # email = EmailOperator(...)
    
    return message


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
    
    check_postgres = PythonOperator(
        task_id='check_postgres_health',
        python_callable=check_postgres_health,
        doc_md="""
        ### Check PostgreSQL Health
        Verifies that PostgreSQL database is running and accessible.
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
    # Task 4: Start Spark Streaming (if not already running)
    # ========================================================================
    # Note: In production, Spark Streaming runs as long-running job
    # This task just checks if it's running and starts if needed
    
    check_spark_streaming = BashOperator(
        task_id='check_spark_streaming',
        bash_command="""
        # Check if Spark Streaming job is running
        if pgrep -f "SparkStreamingConsumer" > /dev/null; then
            echo "✅ Spark Streaming is already running"
        else
            echo "⚠️  Spark Streaming not running, starting..."
            # In production, start as background daemon or use spark-submit
            # nohup python spark_streaming_consumer.py &
            echo "Manual intervention required: Start Spark Streaming"
        fi
        """,
        doc_md="""
        ### Check Spark Streaming
        Verifies that Spark Structured Streaming job is running.
        In production, this should be a long-running job managed separately.
        """
    )
    
    # ========================================================================
    # Task 5: Wait for Spark Processing
    # ========================================================================
    wait_for_processing = BashOperator(
        task_id='wait_for_spark_processing',
        bash_command="""
        echo "⏳ Waiting for Spark to process messages..."
        # Wait 60 seconds for micro-batches to process
        sleep 60
        echo "✅ Processing window complete"
        """,
        doc_md="""
        ### Wait for Processing
        Gives Spark time to process messages from Kafka.
        Spark processes in micro-batches every 10 seconds.
        """
    )
    
    # ========================================================================
    # Task 6: Validate PostgreSQL Data
    # ========================================================================
    validate_data = PythonOperator(
        task_id='validate_postgres_data',
        python_callable=validate_postgres_data,
        doc_md="""
        ### Validate PostgreSQL Data
        Checks that data was successfully written to PostgreSQL.
        Validates record counts, data ranges, and basic metrics.
        """
    )
    
    # ========================================================================
    # Task 7: Run Analytics Queries
    # ========================================================================
    run_analytics = PostgresOperator(
        task_id='run_analytics_queries',
        postgres_conn_id='postgres_reddit',
        sql="""
        -- Refresh materialized views or update analytics tables
        
        -- Top subreddits by volume
        CREATE TEMP TABLE top_subreddits AS
        SELECT 
            subreddit,
            COUNT(*) as post_count,
            AVG(score) as avg_score
        FROM reddit_posts
        WHERE processing_timestamp >= NOW() - INTERVAL '4 hours'
        GROUP BY subreddit
        ORDER BY post_count DESC
        LIMIT 10;
        
        -- Sentiment trends
        CREATE TEMP TABLE sentiment_trends AS
        SELECT 
            DATE_TRUNC('hour', created_datetime) as hour,
            sentiment,
            COUNT(*) as count
        FROM reddit_posts
        WHERE processing_timestamp >= NOW() - INTERVAL '4 hours'
        GROUP BY DATE_TRUNC('hour', created_datetime), sentiment;
        
        -- Log results
        SELECT 'Analytics queries completed' as status;
        """,
        doc_md="""
        ### Run Analytics Queries
        Executes analytical queries on processed data.
        Creates aggregated views for dashboards and reporting.
        """
    )
    
    # ========================================================================
    # Task 8: Generate Data Quality Report
    # ========================================================================
    quality_report = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_data_quality_report,
        doc_md="""
        ### Generate Quality Report
        Analyzes data quality metrics including null values,
        data completeness, and overall quality score.
        """
    )
    
    # ========================================================================
    # Task 9: Send Success Notification
    # ========================================================================
    notify_success = PythonOperator(
        task_id='send_success_notification',
        python_callable=send_success_notification,
        doc_md="""
        ### Send Success Notification
        Sends pipeline completion notification with statistics.
        In production, sends to Slack, email, or monitoring system.
        """
    )
    
    # ========================================================================
    # Task Dependencies (Pipeline Flow)
    # ========================================================================
    
    # Health checks must pass first
    [check_kafka, check_postgres] >> fetch_data
    
    # Fetch data, then produce to Kafka
    fetch_data >> kafka_produce
    
    # Check Spark streaming is running
    kafka_produce >> check_spark_streaming
    
    # Wait for Spark to process
    check_spark_streaming >> wait_for_processing
    
    # Validate data in PostgreSQL
    wait_for_processing >> validate_data
    
    # Run analytics and quality checks in parallel
    validate_data >> [run_analytics, quality_report]
    
    # Send notification after everything completes
    [run_analytics, quality_report] >> notify_success


# ============================================================================
# DAG Documentation
# ============================================================================
dag.doc_md = """
# Reddit Streaming Data Pipeline

## Architecture Overview

```
Reddit API → Kafka → Spark Streaming → PostgreSQL
    ↓          ↓            ↓              ↓
  Fetch    Message     Real-time      OLAP
  Data     Queue      Processing    Database
```

## Pipeline Components

1. **Reddit API**: Fetches latest posts from subreddits
2. **Kafka**: Message queue for reliable data streaming
3. **Spark Structured Streaming**: Real-time ETL processing
4. **PostgreSQL**: Analytical database for storage

## Data Flow

1. Airflow fetches data from Reddit API every 4 hours
2. Data is produced to Kafka topic `reddit-posts`
3. Spark Streaming continuously consumes and processes:
   - Text cleaning
   - Sentiment analysis
   - Topic classification
   - Entity extraction
4. Processed data is written to PostgreSQL
5. Analytics queries run on PostgreSQL
6. Data quality metrics generated
7. Success notification sent

## Monitoring

- Check Airflow UI for DAG runs: http://localhost:8080
- Check Kafka UI for messages: http://localhost:8080 (Kafka UI)
- Check Spark UI for streaming: http://localhost:4040
- Query PostgreSQL for data: `SELECT * FROM reddit_posts`

## Failure Handling

- Automatic retries (2 attempts)
- Email notifications on failure
- Checkpointing in Spark for fault tolerance
- Kafka message persistence

## Configuration

- Schedule: Every 4 hours
- Max concurrent runs: 1
- Timeout: 2 hours
- Kafka topic: `reddit-posts`
- PostgreSQL table: `reddit_posts`

## Dependencies

- Apache Airflow 2.7+
- Apache Kafka 3.5+
- Apache Spark 3.5+
- PostgreSQL 14+
- Python 3.9+
"""
