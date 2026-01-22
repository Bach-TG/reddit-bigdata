"""
SPARK STRUCTURED STREAMING - Kafka Consumer
Mục đích: Consume data từ Kafka, xử lý với Spark, write vào PostgreSQL
Real-time streaming ETL pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp,
    udf, lower, regexp_replace, size, split, when, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    LongType, ArrayType, FloatType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkStreamingConsumer:
    def __init__(
        self,
        kafka_bootstrap_servers='localhost:9092',
        kafka_topic='reddit-posts',
        postgres_url='jdbc:postgresql://localhost:5432/reddit_db',
        postgres_user='reddit_user',
        postgres_password='reddit_pass',
        checkpoint_location='/tmp/spark-checkpoint'
    ):
        """
        Initialize Spark Structured Streaming consumer
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            kafka_topic: Kafka topic to consume from
            postgres_url: PostgreSQL JDBC URL
            postgres_user: PostgreSQL username
            postgres_password: PostgreSQL password
            checkpoint_location: Directory for Spark checkpoints
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.postgres_url = postgres_url
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.checkpoint_location = checkpoint_location
        
        # Initialize Spark Session with Kafka and PostgreSQL support
        self.spark = SparkSession.builder \
            .appName("RedditStreamingETL") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.streaming.checkpointLocation", checkpoint_location) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "10") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark Streaming initialized - Kafka: {kafka_bootstrap_servers}")
        logger.info(f"PostgreSQL: {postgres_url}")
    
    def define_schema(self):
        """Define schema for Reddit posts"""
        return StructType([
            StructField("post_id", StringType(), False),
            StructField("subreddit", StringType(), False),
            StructField("title", StringType(), False),
            StructField("body", StringType(), True),
            StructField("author", StringType(), False),
            StructField("created_utc", LongType(), False),
            StructField("score", IntegerType(), False),
            StructField("kafka_timestamp", StringType(), True),
            StructField("producer_id", StringType(), True)
        ])
    
    def register_udfs(self):
        """Register UDFs for data processing"""
        
        # Clean text UDF
        def clean_text_func(text):
            if not text:
                return ""
            import re
            text = text.lower()
            text = re.sub(r'http\S+|www.\S+', '', text)
            text = re.sub(r'[^\w\s\.\,\!\?\-]', '', text)
            return ' '.join(text.split()).strip()
        
        # Sentiment analysis UDF (simple)
        def sentiment_func(text):
            if not text:
                return 0.0
            positive = ['good', 'great', 'support', 'peace', 'success', 'win']
            negative = ['war', 'conflict', 'death', 'attack', 'ban', 'threat']
            
            text_lower = text.lower()
            pos_count = sum(1 for w in positive if w in text_lower)
            neg_count = sum(1 for w in negative if w in text_lower)
            
            total = pos_count + neg_count
            if total == 0:
                return 0.0
            return float(pos_count - neg_count) / total
        
        # Topic classification UDF
        def classify_topic_func(text):
            if not text:
                return 'other'
            text = text.lower()
            
            if any(w in text for w in ['war', 'military', 'troops', 'defense']):
                return 'war_conflict'
            elif any(w in text for w in ['trump', 'president', 'election', 'minister']):
                return 'politics'
            elif any(w in text for w in ['trade', 'economy', 'deal']):
                return 'trade_economy'
            elif any(w in text for w in ['ban', 'restrict', 'law']):
                return 'policy'
            else:
                return 'other'
        
        # Extract entities UDF
        def extract_entities_func(text):
            if not text:
                return []
            countries = ['usa', 'us', 'china', 'russia', 'ukraine', 'israel',
                        'uk', 'iran', 'india', 'pakistan', 'greenland']
            text = text.lower()
            return [c for c in countries if c in text]
        
        # Register UDFs
        clean_text_udf = udf(clean_text_func, StringType())
        sentiment_udf = udf(sentiment_func, FloatType())
        topic_udf = udf(classify_topic_func, StringType())
        entities_udf = udf(extract_entities_func, ArrayType(StringType()))
        
        return {
            'clean_text': clean_text_udf,
            'sentiment': sentiment_udf,
            'topic': topic_udf,
            'entities': entities_udf
        }
    
    def read_kafka_stream(self):
        """Read streaming data from Kafka"""
        logger.info(f"Reading from Kafka topic: {self.kafka_topic}")
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        return df
    
    def process_stream(self, df):
        """Process streaming data with transformations"""
        logger.info("Applying transformations...")
        
        # Parse JSON from Kafka value
        schema = self.define_schema()
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_ingestion_time")
        ).select("data.*", "kafka_ingestion_time")
        
        # Register UDFs
        udfs = self.register_udfs()
        
        # Add processing timestamp
        processed_df = parsed_df.withColumn(
            "processing_timestamp", 
            current_timestamp()
        )
        
        # Convert Unix timestamp to datetime
        processed_df = processed_df.withColumn(
            "created_datetime",
            to_timestamp(col("created_utc"))
        )
        
        # Text cleaning
        processed_df = processed_df \
            .withColumn("title_cleaned", udfs['clean_text'](col("title"))) \
            .withColumn("body_cleaned", udfs['clean_text'](col("body")))
        
        # Sentiment analysis
        processed_df = processed_df.withColumn(
            "sentiment_score",
            udfs['sentiment'](col("title_cleaned"))
        )
        
        processed_df = processed_df.withColumn(
            "sentiment",
            when(col("sentiment_score") > 0.1, "positive")
            .when(col("sentiment_score") < -0.1, "negative")
            .otherwise("neutral")
        )
        
        # Topic classification
        processed_df = processed_df.withColumn(
            "topic",
            udfs['topic'](col("title_cleaned"))
        )
        
        # Entity extraction
        processed_df = processed_df.withColumn(
            "entities",
            udfs['entities'](col("title"))
        )
        
        processed_df = processed_df.withColumn(
            "entity_count",
            size(col("entities"))
        )
        
        # Text features
        processed_df = processed_df \
            .withColumn("title_word_count", 
                       size(split(col("title_cleaned"), " "))) \
            .withColumn("has_body", 
                       col("body_cleaned").isNotNull() & 
                       (col("body_cleaned") != ""))
        
        # Engagement category
        processed_df = processed_df.withColumn(
            "engagement_category",
            when(col("score") < 10, "low")
            .when(col("score") < 50, "medium")
            .when(col("score") < 100, "high")
            .otherwise("viral")
        )
        
        logger.info("Transformations applied")
        return processed_df
    
    def write_to_postgres_batch(self, batch_df, batch_id):
        """
        Write each micro-batch to PostgreSQL
        
        Args:
            batch_df: Micro-batch DataFrame
            batch_id: Batch identifier
        """
        try:
            logger.info(f"Writing batch {batch_id} to PostgreSQL...")
            
            # Convert array to string for PostgreSQL (arrays need special handling)
            batch_df = batch_df.withColumn(
                "entities_str",
                expr("concat('[', concat_ws(',', entities), ']')")
            )
            
            # Select final columns for PostgreSQL
            final_df = batch_df.select(
                "post_id",
                "subreddit",
                "title",
                "title_cleaned",
                "body",
                "body_cleaned",
                "author",
                "created_utc",
                "created_datetime",
                "score",
                "sentiment_score",
                "sentiment",
                "topic",
                "entities_str",
                "entity_count",
                "title_word_count",
                "has_body",
                "engagement_category",
                "kafka_ingestion_time",
                "processing_timestamp"
            )
            
            # Write to PostgreSQL
            final_df.write \
                .format("jdbc") \
                .option("url", self.postgres_url) \
                .option("dbtable", "reddit_posts") \
                .option("user", self.postgres_user) \
                .option("password", self.postgres_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            count = final_df.count()
            logger.info(f"✅ Batch {batch_id} written: {count} records")
            
        except Exception as e:
            logger.error(f"❌ Error writing batch {batch_id}: {e}")
            raise
    
    def start_streaming(self, output_mode="append"):
        """
        Start the streaming pipeline
        
        Args:
            output_mode: Spark streaming output mode (append, update, complete)
        """
        logger.info("="*70)
        logger.info("🚀 Starting Spark Structured Streaming Pipeline")
        logger.info("="*70)
        
        # Read from Kafka
        raw_stream = self.read_kafka_stream()
        
        # Process stream
        processed_stream = self.process_stream(raw_stream)
        
        # Write to PostgreSQL using foreachBatch
        query = processed_stream.writeStream \
            .outputMode(output_mode) \
            .foreachBatch(self.write_to_postgres_batch) \
            .option("checkpointLocation", self.checkpoint_location) \
            .trigger(processingTime='10 seconds') \
            .start()
        
        logger.info("✅ Streaming pipeline started")
        logger.info(f"📊 Output mode: {output_mode}")
        logger.info(f"📁 Checkpoint: {self.checkpoint_location}")
        logger.info(f"⏱️  Trigger: Every 10 seconds")
        logger.info("\nPress Ctrl+C to stop...\n")
        
        # Wait for termination
        query.awaitTermination()
    
    def stop(self):
        """Stop Spark session"""
        logger.info("Stopping Spark session...")
        self.spark.stop()

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Spark Streaming from Kafka to PostgreSQL')
    parser.add_argument(
        '--kafka-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers'
    )
    parser.add_argument(
        '--kafka-topic',
        default='reddit-posts',
        help='Kafka topic to consume'
    )
    parser.add_argument(
        '--postgres-url',
        default='jdbc:postgresql://localhost:5432/reddit_db',
        help='PostgreSQL JDBC URL'
    )
    parser.add_argument(
        '--postgres-user',
        default='reddit_user',
        help='PostgreSQL username'
    )
    parser.add_argument(
        '--postgres-password',
        default='reddit_pass',
        help='PostgreSQL password'
    )
    parser.add_argument(
        '--checkpoint',
        default='/tmp/spark-checkpoint',
        help='Checkpoint location'
    )
    
    args = parser.parse_args()
    
    # Initialize consumer
    consumer = SparkStreamingConsumer(
        kafka_bootstrap_servers=args.kafka_servers,
        kafka_topic=args.kafka_topic,
        postgres_url=args.postgres_url,
        postgres_user=args.postgres_user,
        postgres_password=args.postgres_password,
        checkpoint_location=args.checkpoint
    )
    
    try:
        # Start streaming
        consumer.start_streaming()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Pipeline interrupted by user")
    finally:
        consumer.stop()
