"""
RAW LAYER (BRONZE) - PySpark Version
Mục đích: Thu thập và lưu trữ dữ liệu gốc từ Reddit API
Scalable to millions of records with Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, year, month, dayofmonth,
    md5, concat_ws, unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from datetime import datetime
from pathlib import Path
import json
from pyspark.sql import functions as F
from db_connection_config import DB_CONFIG

# 10. **spark_01_raw_layer.py** ⚡
#     - PySpark: Raw Data Ingestion
#     - Distributed JSONL loading
#     - Parallel partitioning
#     - ~8 KB, 250 dòng
#     - **Chạy đầu tiên (PySpark)**


class RawLayerProcessorSpark:
    def __init__(self, input_file, output_dir="data_spark/raw"):
        self.input_file = input_file
        self.output_dir = output_dir
        self.batch_id = self._generate_batch_id()
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("RedditRawLayer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"✅ Spark Session created: {self.spark.version}")
    
    def _generate_batch_id(self):
        """Tạo unique batch ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"batch_{timestamp}"
    
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
    
    def load_jsonl(self):
        """Load JSONL file using Spark"""
        schema = self.define_schema()
        
        # Read JSONL
        df = self.spark.read \
            .option("multiline", "false") \
            .schema(schema) \
            .json(self.input_file)
        
        print(f"📥 Loaded {df.count()} records from JSONL")
        return df
    
    def add_metadata(self, df):
        """Thêm metadata cho raw data"""
        df = df.withColumn("ingestion_timestamp", current_timestamp()) \
               .withColumn("batch_id", lit(self.batch_id)) \
               .withColumn("source_api", lit("reddit")) \
               .withColumn("data_quality_flag", lit("raw"))
        
        # Tạo unique row hash
        df = df.withColumn(
            "row_hash",
            md5(concat_ws("_", col("post_id"), col("created_utc").cast("string")))
        )
        
        print("✅ Added metadata columns")
        return df
    
    def add_partition_columns(self, df):
        """Tạo partition columns từ timestamp"""
        # Convert Unix timestamp to datetime
        df = df.withColumn("created_datetime", 
                          (col("created_utc").cast("timestamp")))
        
        # Extract partition columns
        df = df.withColumn("partition_date", to_date("created_datetime"))
        df = df.withColumn("partition_year", year("created_datetime"))
        df = df.withColumn("partition_month", month("created_datetime"))
        df = df.withColumn("partition_day", dayofmonth("created_datetime"))
        
        print("✅ Added partition columns")
        return df
    
    def save_to_parquet(self, df):
        """Lưu dữ liệu theo partition structure với Spark"""
        # Write partitioned Parquet
        df.write \
            .mode("overwrite") \
            .partitionBy("subreddit", "partition_year", "partition_month", "partition_day") \
            .option("compression", "snappy") \
            .parquet(self.output_dir)
        
        print(f"💾 Saved partitioned data to {self.output_dir}")
        
        # Count records per partition
        partition_counts = df.groupBy("subreddit", "partition_year", "partition_month", "partition_day") \
            .count() \
            .orderBy("subreddit", "partition_year", "partition_month", "partition_day")
        
        print("\n📊 Records per partition:")
        partition_counts.show(50, False)
    
    def generate_manifest(self, df):
        """Tạo manifest file để track batches"""
        # Collect statistics using Spark
        total_records = df.count()
        subreddit_counts = df.groupBy("subreddit").count().collect()
        
        # Date range
        date_stats = df.agg(
            F.min("created_datetime").alias("min"),
            F.max("created_datetime").alias("max")
        ).collect()[0]
        
        manifest = {
            'batch_id': self.batch_id,
            'ingestion_time': datetime.now().isoformat(),
            'source_file': str(self.input_file),
            'total_records': total_records,
            'subreddits': {row['subreddit']: row['count'] for row in subreddit_counts},
            'date_range': {
                'min': str(date_stats['min']),
                'max': str(date_stats['max'])
            },
            'schema_version': '1.0',
            'spark_version': self.spark.version
        }
        
        # Save manifest
        manifest_dir = Path(self.output_dir).parent / 'manifests'
        manifest_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = manifest_dir / f"{self.batch_id}_manifest.json"
        
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        print(f"\n📋 Manifest created: {manifest_path}")
        return manifest
    
    def process(self):
        """Main processing pipeline"""
        print("="*70)
        print(f"🚀 Starting Raw Layer Processing (PySpark) - Batch: {self.batch_id}")
        print("="*70)
        
        # Step 1: Load data
        print("\n📥 Step 1: Loading JSONL data with Spark...")
        df = self.load_jsonl()
        
        # Step 2: Add metadata
        print("\n🏷️  Step 2: Adding metadata...")
        df = self.add_metadata(df)
        
        # Step 3: Add partition columns
        print("\n📊 Step 3: Creating partition columns...")
        df = self.add_partition_columns(df)
        
        # Cache for performance
        df.cache()
        
        # Step 4: Save to Parquet
        print("\n💾 Step 4: Saving to Parquet format...")
        self.save_to_parquet(df)

        # Step 4b: Save to PostgreSQL
        print("\n🐘 Step 4b: Saving to PostgreSQL...")
        self.save_to_postgresql(df, "raw_reddit_posts")

        # Step 5: Generate manifest
        print("\n📝 Step 5: Generating manifest...")
        manifest = self.generate_manifest(df)
        
        # Show sample
        print("\n📋 Sample Raw Data:")
        df.select("post_id", "subreddit", "title", "score", "created_datetime") \
          .show(10, truncate=60)
        
        # Statistics
        print("\n📊 Raw Layer Statistics:")
        df.groupBy("subreddit") \
          .agg(
              {"post_id": "count", "score": "avg"}
          ) \
          .withColumnRenamed("count(post_id)", "total_posts") \
          .withColumnRenamed("avg(score)", "avg_score") \
          .orderBy("total_posts", ascending=False) \
          .show()
        
        print("\n" + "="*70)
        print("✨ Raw Layer Processing Complete!")
        print(f"📦 Total Records: {manifest['total_records']}")
        print(f"📅 Date Range: {manifest['date_range']['min']} to {manifest['date_range']['max']}")
        print(f"🗂️  Subreddits: {', '.join(manifest['subreddits'].keys())}")
        
        # Unpersist cache
        df.unpersist()
        
        return df, manifest
    
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

    def save_to_postgresql(self, df, table_name="raw_reddit_posts"):
        """Save DataFrame to PostgreSQL table"""
        jdbc_url = self.get_jdbc_url()
        properties = self.get_properties()

        # Write to PostgreSQL
        df.write \
          .mode("overwrite") \
          .jdbc(url=jdbc_url, table=table_name, properties=properties)

        print(f"✅ Data saved to PostgreSQL table: {table_name}")

    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        print("🛑 Spark session stopped")


# ============================================================================
# EXECUTION
# ============================================================================
if __name__ == "__main__":
    # Initialize processor
    processor = RawLayerProcessorSpark(
        input_file="/mnt/user-data/uploads/reddit_posts.jsonl",
        output_dir="data_spark/raw"
    )

    try:
        # Run processing
        df, manifest = processor.process()

        print("\n✅ Raw Layer Complete - Data ready for Processed Layer")

        # Optionally save to PostgreSQL as well
        processor.save_to_postgresql(df, "raw_reddit_posts")

    finally:
        # Always stop Spark
        processor.stop()
