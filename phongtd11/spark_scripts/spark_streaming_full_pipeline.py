"""
STREAMING PIPELINE - PySpark Version
Mục đích: Real-time processing pipeline consuming from Kafka and processing through all layers
Real-time streaming data ingestion with Spark Structured Streaming through Raw -> Processed -> Analytics layers
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, lower, trim, regexp_replace, when, length, split, size,
    array, lit, hour, dayofweek, date_format, explode, current_timestamp, 
    to_date, year, month, dayofmonth, md5, concat_ws, from_json
)
from pyspark.sql.types import (
    StringType, FloatType, IntegerType, BooleanType, ArrayType, 
    StructType, StructField, LongType
)
import re
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Database configuration for PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'your_database_name',
    'username': 'your_username',
    'password': 'your_password'
}


class StreamingPipelineProcessor:
    def __init__(self, kafka_bootstrap_servers="localhost:9092", kafka_topic="reddit_posts", 
                 raw_output_dir="data_spark/raw_streaming",
                 processed_output_dir="data_spark/processed_streaming",
                 analytics_output_dir="data_spark/analytics_streaming"):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.raw_output_dir = raw_output_dir
        self.processed_output_dir = processed_output_dir
        self.analytics_output_dir = analytics_output_dir

        # Initialize Spark Session with Kafka support
        self.spark = SparkSession.builder \
            .appName("RedditStreamingPipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()

        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")

        # Topic keywords for NLP processing
        self.topic_keywords = {
            'war_conflict': ['war', 'military', 'conflict', 'attack', 'troops', 'defense', 'battle', 'invasion'],
            'trade_economy': ['trade', 'economy', 'gdp', 'tariff', 'export', 'import', 'market', 'deal'],
            'politics': ['election', 'president', 'government', 'minister', 'political', 'vote', 'congress'],
            'climate': ['climate', 'environment', 'carbon', 'renewable', 'pollution', 'emissions'],
            'technology': ['tech', 'ai', 'cyber', 'digital', 'software', 'data', 'internet'],
            'healthcare': ['health', 'covid', 'vaccine', 'hospital', 'medical', 'pandemic'],
            'diplomacy': ['diplomat', 'foreign', 'embassy', 'treaty', 'alliance', 'summit'],
            'migration': ['refugee', 'migration', 'border', 'asylum', 'immigrant']
        }

        # Countries for entity extraction
        self.countries = [
            'usa', 'us', 'america', 'china', 'russia', 'ukraine', 'israel', 'palestine',
            'uk', 'britain', 'france', 'germany', 'india', 'pakistan', 'iran', 'turkey',
            'saudi', 'canada', 'australia', 'japan', 'korea', 'mexico', 'brazil', 'greenland'
        ]

        print(f"✅ Spark Session created: {self.spark.version}")

    def define_schema(self):
        """Define explicit schema for Reddit posts"""
        return StructType([
            StructField("post_id", StringType(), False),
            StructField("subreddit", StringType(), False),
            StructField("title", StringType(), False),
            StructField("body", StringType(), True),
            StructField("author", StringType(), False),
            StructField("created_utc", LongType(), False),
            StructField("score", IntegerType(), False)
        ])

    def load_from_kafka(self):
        """Load data from Kafka topic using Spark Structured Streaming"""
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .load()

        # Parse the JSON value field
        reddit_schema = self.define_schema()
        parsed_df = df.withColumn("parsed_value", from_json(col("value").cast("string"), reddit_schema)) \
                      .select("parsed_value.*", "timestamp", "offset")

        print(f"📥 Configured to consume from Kafka topic: {self.kafka_topic}")
        return parsed_df

    def add_raw_layer_metadata(self, df):
        """Add metadata for raw layer"""
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("source_api", lit("kafka_reddit")) \
               .withColumn("data_quality_flag", lit("raw_streaming"))

        # Tạo unique row hash
        df = df.withColumn(
            "row_hash",
            md5(concat_ws("_", col("post_id"), col("created_utc").cast("string"), col("ingestion_timestamp").cast("string")))
        )

        # Convert Unix timestamp to datetime
        df = df.withColumn("created_datetime",
                          (col("created_utc").cast("timestamp")))

        # Extract partition columns
        df = df.withColumn("partition_date", to_date("created_datetime"))
        df = df.withColumn("partition_year", year("created_datetime"))
        df = df.withColumn("partition_month", month("created_datetime"))
        df = df.withColumn("partition_day", dayofmonth("created_datetime"))

        print("✅ Added raw layer metadata and partition columns")
        return df

    def register_udfs(self):
        """Register User Defined Functions for NLP tasks"""

        # UDF 1: Clean text
        def clean_text_func(text):
            if not text:
                return ""
            # Convert to lowercase
            text = text.lower()
            # Remove URLs
            text = re.sub(r'http\S+|www.\S+', '', text)
            # Remove special characters
            text = re.sub(r'[^\w\s\.\,\!\?\-]', '', text)
            # Remove extra whitespace
            text = ' '.join(text.split())
            return text.strip()

        clean_text_udf = udf(clean_text_func, StringType())

        # UDF 2: Simple sentiment (keyword-based)
        positive_words = ['good', 'great', 'support', 'peace', 'agreement', 'success', 'win', 'positive']
        negative_words = ['war', 'conflict', 'death', 'attack', 'ban', 'restrict', 'threat', 'crisis']

        def sentiment_polarity_func(text):
            if not text:
                return 0.0
            text = text.lower()
            pos_count = sum(1 for word in positive_words if word in text)
            neg_count = sum(1 for word in negative_words if word in text)

            total = pos_count + neg_count
            if total == 0:
                return 0.0
            return (pos_count - neg_count) / total

        def sentiment_label_func(polarity):
            if polarity > 0.1:
                return 'positive'
            elif polarity < -0.1:
                return 'negative'
            else:
                return 'neutral'

        sentiment_polarity_udf = udf(sentiment_polarity_func, FloatType())
        sentiment_label_udf = udf(sentiment_label_func, StringType())

        topic_keywords_bc = self.spark.sparkContext.broadcast(self.topic_keywords)
        countries_bc = self.spark.sparkContext.broadcast(self.countries)

        # UDF 3: Topic classification
        def classify_topic_func(text):
            if not text:
                return ['other']
            text = text.lower()
            detected = []

            for topic, keywords in topic_keywords_bc.value.items():
                if any(kw in text for kw in keywords):
                    detected.append(topic)

            return detected if detected else ['other']

        classify_topic_udf = udf(classify_topic_func, ArrayType(StringType()))

        # UDF 4: Extract entities
        def extract_entities_func(text):
            if not text:
                return []
            text = text.lower()
            found = [c for c in countries_bc.value if c in text]
            return list(set(found))  # Remove duplicates

        extract_entities_udf = udf(extract_entities_func, ArrayType(StringType()))

        return {
            'clean_text': clean_text_udf,
            'sentiment_polarity': sentiment_polarity_udf,
            'sentiment_label': sentiment_label_udf,
            'classify_topic': classify_topic_udf,
            'extract_entities': extract_entities_udf
        }

    def process_raw_to_processed(self, df):
        """Process raw data to processed data using NLP techniques"""
        print("🔄 Processing raw data to processed layer...")
        
        # Register UDFs
        udfs = self.register_udfs()

        # Step 1: Text cleaning
        df = df.withColumn("title_cleaned", udfs['clean_text'](col("title"))) \
               .withColumn("body_cleaned", udfs['clean_text'](col("body")))

        print("   ✓ Text cleaned and normalized")

        # Step 2: Sentiment analysis
        df = df.withColumn("sentiment_polarity", udfs['sentiment_polarity'](col("title_cleaned"))) \
               .withColumn("sentiment", udfs['sentiment_label'](col("sentiment_polarity"))) \
               .withColumn("sentiment_subjectivity", lit(0.5))  # Simplified

        # Step 3: Topic classification
        df = df.withColumn("topics", udfs['classify_topic'](col("title_cleaned"))) \
               .withColumn("primary_topic", col("topics")[0])

        # Step 4: Entity extraction
        df = df.withColumn("entities", udfs['extract_entities'](col("title"))) \
               .withColumn("entity_count", size(col("entities")))

        # Step 5: Text features
        df = df.withColumn("title_word_count", size(split(col("title_cleaned"), " "))) \
               .withColumn("body_word_count",
                          when(col("body_cleaned").isNotNull(),
                               size(split(col("body_cleaned"), " ")))
                          .otherwise(0)) \
               .withColumn("has_body", col("body_word_count") > 0) \
               .withColumn("title_char_count", length(col("title_cleaned")))

        # Step 6: Time features
        df = df.withColumn("hour_of_day", hour("created_datetime")) \
               .withColumn("day_of_week", dayofweek("created_datetime")) \
               .withColumn("day_name", date_format("created_datetime", "EEEE")) \
               .withColumn("is_weekend", col("day_of_week").isin([1, 7]))  # 1=Sunday, 7=Saturday

        # Time period
        df = df.withColumn("time_period",
                          when(col("hour_of_day").between(0, 6), "night")
                          .when(col("hour_of_day").between(7, 12), "morning")
                          .when(col("hour_of_day").between(13, 18), "afternoon")
                          .otherwise("evening"))

        # Engagement category
        df = df.withColumn("engagement_category",
                          when(col("score") >= 1000, "viral")
                          .when(col("score") >= 100, "popular")
                          .when(col("score") >= 10, "normal")
                          .otherwise("low"))

        # Normalized score
        df = df.withColumn("score_normalized", 
                          (col("score") - F.min("score").over(Window.partitionBy())) / 
                          (F.max("score").over(Window.partitionBy()) - F.min("score").over(Window.partitionBy())) * 100)

        print("   ✅ Processed layer transformation complete")
        return df

    def create_analytics_tables(self, df):
        """Create analytics tables from processed data"""
        print("📊 Creating analytics tables...")

        # Daily subreddit statistics
        daily_stats = df.groupBy("subreddit", F.to_date("created_datetime").alias("date")) \
            .agg(
                F.count("*").alias("total_posts"),
                F.sum("score").alias("total_score"),
                F.round(F.avg("score"), 2).alias("avg_score"),
                F.percentile_approx("score", 0.5).alias("median_score"),
                F.max("score").alias("max_score"),
                F.round(F.stddev("score"), 2).alias("score_std"),
                F.round(F.avg("sentiment_polarity"), 3).alias("avg_sentiment"),
                F.round(F.avg("title_word_count"), 1).alias("avg_title_length"),
                F.round(F.avg("entity_count"), 1).alias("avg_entity_count"),
                F.sum(F.when(col("has_body"), 1).otherwise(0)).alias("posts_with_body")
            ) \
            .orderBy("subreddit", "date")

        # Add growth rate using window function
        window_spec = Window.partitionBy("subreddit").orderBy("date")
        daily_stats = daily_stats.withColumn(
            "posts_growth_rate",
            F.round(
                (col("total_posts") - F.lag("total_posts").over(window_spec)) /
                F.lag("total_posts").over(window_spec) * 100,
                2
            )
        )

        print("   ✅ Daily subreddit statistics created")
        return {"daily_stats": daily_stats}

    def get_jdbc_url(self):
        """Get JDBC URL for PostgreSQL"""
        return f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

    def get_properties(self):
        """Get connection properties for PostgreSQL"""
        properties = {
            "user": DB_CONFIG['username'],
            "password": DB_CONFIG['password'],
            "driver": "org.postgresql.Driver"
        }
        return properties

    def save_to_postgresql(self, df, table_name):
        """Save DataFrame to PostgreSQL table using foreachBatch"""
        def save_batch(batch_df, batch_id):
            if batch_df.count() > 0:
                jdbc_url = self.get_jdbc_url()
                properties = self.get_properties()

                # Write to PostgreSQL
                batch_df.write \
                  .mode("append") \
                  .jdbc(url=jdbc_url, table=table_name, properties=properties)

                print(f"✅ Batch {batch_id}: Data saved to PostgreSQL table: {table_name}")
        
        # Apply the function to each micro-batch
        query = df.writeStream \
            .foreachBatch(save_batch) \
            .trigger(processingTime='30 seconds') \
            .outputMode("update") \
            .start()

        print(f"🐘 Started streaming to PostgreSQL table: {table_name}")
        return query

    def process_streaming_pipeline(self):
        """Main streaming pipeline processing"""
        print("="*70)
        print("🚀 Starting Streaming Pipeline Processing (Raw -> Processed -> Analytics)")
        print("="*70)

        # Step 1: Load data from Kafka
        print(f"\n📥 Step 1: Loading data from Kafka topic '{self.kafka_topic}'...")
        raw_df = self.load_from_kafka()

        # Step 2: Add raw layer metadata
        print("\n🏷️  Step 2: Adding raw layer metadata...")
        raw_df = self.add_raw_layer_metadata(raw_df)

        # Step 3: Process to processed layer
        print("\n🔄 Step 3: Processing to processed layer...")
        processed_df = self.process_raw_to_processed(raw_df)

        # Step 4: Create analytics tables
        print("\n📊 Step 4: Creating analytics tables...")
        analytics_tables = self.create_analytics_tables(processed_df)

        # Step 5: Set up streaming to storage and databases
        print("\n💾 Step 5: Setting up streaming destinations...")

        # Stream raw data to Parquet
        raw_query = raw_df.writeStream \
            .trigger(processingTime='30 seconds') \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.raw_output_dir}/checkpoint") \
            .partitionBy("subreddit", "partition_year", "partition_month", "partition_day") \
            .option("path", self.raw_output_dir) \
            .format("parquet") \
            .start()

        # Stream processed data to Parquet
        processed_query = processed_df.writeStream \
            .trigger(processingTime='30 seconds') \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.processed_output_dir}/checkpoint") \
            .partitionBy("subreddit", "partition_year", "partition_month", "partition_day") \
            .option("path", self.processed_output_dir) \
            .format("parquet") \
            .start()

        # Stream analytics data to Parquet
        analytics_query = analytics_tables["daily_stats"].writeStream \
            .trigger(processingTime='60 seconds') \
            .outputMode("append") \
            .option("checkpointLocation", f"{self.analytics_output_dir}/checkpoint/daily_stats") \
            .partitionBy("subreddit") \
            .option("path", f"{self.analytics_output_dir}/daily_stats") \
            .format("parquet") \
            .start()

        # Stream to PostgreSQL tables
        raw_postgres_query = self.save_to_postgresql(raw_df, "raw_reddit_posts")
        processed_postgres_query = self.save_to_postgresql(processed_df, "processed_reddit_posts")
        analytics_postgres_query = self.save_to_postgresql(analytics_tables["daily_stats"], "daily_subreddit_stats")

        print(f"\n📡 Streaming pipeline started. Listening to Kafka topic: {self.kafka_topic}")
        print("Press Ctrl+C to stop the streaming pipeline...")
        
        try:
            # Wait for all queries to terminate
            raw_query.awaitTermination()
            processed_query.awaitTermination()
            analytics_query.awaitTermination()
            raw_postgres_query.awaitTermination()
            processed_postgres_query.awaitTermination()
            analytics_postgres_query.awaitTermination()
        except KeyboardInterrupt:
            print("\n🛑 Stopping streaming pipeline...")
            raw_query.stop()
            processed_query.stop()
            analytics_query.stop()
            raw_postgres_query.stop()
            processed_postgres_query.stop()
            analytics_postgres_query.stop()

        return raw_query, processed_query, analytics_query

    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        print("🛑 Spark session stopped")


# ============================================================================
# EXECUTION
# ============================================================================
if __name__ == "__main__":
    # Initialize processor
    processor = StreamingPipelineProcessor(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="reddit_posts",
        raw_output_dir="data_spark/raw_streaming",
        processed_output_dir="data_spark/processed_streaming",
        analytics_output_dir="data_spark/analytics_streaming"
    )

    try:
        # Run streaming processing
        raw_query, processed_query, analytics_query = processor.process_streaming_pipeline()
        print("\n✅ Streaming Pipeline Processing Complete")

    finally:
        # Always stop Spark
        processor.stop()