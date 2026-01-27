"""
PROCESSED LAYER (SILVER) - PySpark Version
Mục đích: Làm sạch, chuẩn hóa và làm giàu dữ liệu với NLP
Scalable NLP processing with Spark UDFs
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, lower, trim, regexp_replace, when, length, split, size,
    array, lit, hour, dayofweek, date_format, explode
)
from pyspark.sql.types import (
    StringType, FloatType, IntegerType, BooleanType, ArrayType, StructType, StructField
)
import re
from pyspark.sql import functions as F
from config import DB_CONFIG

# 11. **spark_02_processed_layer.py** ⚡
#     - PySpark: NLP with UDFs
#     - Distributed sentiment analysis
#     - Parallel topic classification
#     - ~13 KB, 350 dòng
#     - **Chạy sau Raw Layer (PySpark)**


class ProcessedLayerProcessorSpark:
    def __init__(self, raw_data_path="data_spark/raw", output_dir="data_spark/processed"):
        self.raw_data_path = raw_data_path
        self.output_dir = output_dir
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("RedditProcessedLayer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Topic keywords
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
    
    def load_raw_data(self):
        """Load all raw parquet files using Spark"""
        df = self.spark.read.parquet(self.raw_data_path)
        record_count = df.count()
        print(f"📂 Loaded {record_count} records from raw layer")
        return df
    
    def deduplicate(self, df):
        """Remove duplicate posts"""
        before = df.count()
        df = df.dropDuplicates(["post_id"])
        after = df.count()
        removed = before - after
        
        if removed > 0:
            print(f"🗑️  Removed {removed} duplicate posts")
        
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
    
    def process(self):
        """Main processing pipeline"""
        print("="*70)
        print("🔄 Starting Processed Layer Processing (PySpark)")
        print("="*70)
        
        # Step 1: Load raw data
        print("\n📥 Step 1: Loading raw data...")
        df = self.load_raw_data()
        
        # Step 2: Deduplication
        print("\n🔍 Step 2: Removing duplicates...")
        df = self.deduplicate(df)
        
        # Step 3: Register UDFs
        print("\n🔧 Step 3: Registering UDFs for NLP...")
        udfs = self.register_udfs()
        
        # Step 4: Text cleaning
        print("\n🧹 Step 4: Cleaning text...")
        df = df.withColumn("title_cleaned", udfs['clean_text'](col("title"))) \
               .withColumn("body_cleaned", udfs['clean_text'](col("body")))
        
        print("   ✓ Text cleaned and normalized")
        
        # Step 5: Sentiment analysis
        print("\n😊 Step 5: Analyzing sentiment...")
        df = df.withColumn("sentiment_polarity", udfs['sentiment_polarity'](col("title_cleaned"))) \
               .withColumn("sentiment", udfs['sentiment_label'](col("sentiment_polarity"))) \
               .withColumn("sentiment_subjectivity", lit(0.5))  # Simplified
        
        # Show sentiment distribution
        print("   ✓ Sentiment distribution:")
        df.groupBy("sentiment").count().show()
        
        # Step 6: Topic classification
        print("\n📚 Step 6: Classifying topics...")
        df = df.withColumn("topics", udfs['classify_topic'](col("title_cleaned"))) \
               .withColumn("primary_topic", col("topics")[0])
        
        # Show topic distribution
        print("   ✓ Topic distribution:")
        df.groupBy("primary_topic").count().orderBy("count", ascending=False).show()
        
        # Step 7: Entity extraction
        print("\n🌍 Step 7: Extracting entities...")
        df = df.withColumn("entities", udfs['extract_entities'](col("title"))) \
               .withColumn("entity_count", size(col("entities")))
        
        # Top entities
        print("   ✓ Top mentioned entities:")
        df.select(explode("entities").alias("entity")) \
          .groupBy("entity") \
          .count() \
          .orderBy("count", ascending=False) \
          .show(10)
        
        # Step 8: Text features
        print("\n📝 Step 8: Calculating text features...")
        df = df.withColumn("title_word_count", size(split(col("title_cleaned"), " "))) \
               .withColumn("body_word_count", 
                          when(col("body_cleaned").isNotNull(), 
                               size(split(col("body_cleaned"), " ")))
                          .otherwise(0)) \
               .withColumn("has_body", col("body_word_count") > 0) \
               .withColumn("title_char_count", length(col("title_cleaned")))
        
        avg_title_len = df.agg({"title_word_count": "avg"}).collect()[0][0]
        posts_with_body = df.filter(col("has_body")).count()
        print(f"   ✓ Avg title length: {avg_title_len:.1f} words")
        print(f"   ✓ Posts with body: {posts_with_body} ({posts_with_body/df.count()*100:.1f}%)")
        
        # Step 9: Time features
        print("\n⏰ Step 9: Adding time features...")
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
        
        print("   ✓ Time periods:")
        df.groupBy("time_period").count().show()
        
        # Step 10: Engagement metrics
        print("\n📈 Step 10: Calculating engagement metrics...")
        
        # Get min/max for normalization
        score_stats = df.agg(
            F.min("score").alias("min"),
            F.max("score").alias("max")
        ).collect()[0]

        min_score = score_stats['min']
        max_score = score_stats['max']
        
        # Normalize score (avoid division by zero)
        if max_score > min_score:
            df = df.withColumn("score_normalized",
                              ((col("score") - lit(min_score)) / 
                               (lit(max_score) - lit(min_score)) * 100))
        else:
            df = df.withColumn("score_normalized", lit(0))
        
        # Engagement category
        df = df.withColumn("engagement_category",
                          when(col("score") < 10, "low")
                          .when(col("score") < 50, "medium")
                          .when(col("score") < 100, "high")
                          .otherwise("viral"))
        
        print("   ✓ Engagement categories:")
        df.groupBy("engagement_category").count().show()
        
        # Step 11: Save processed data
        print("\n💾 Step 11: Saving processed data...")
        
        # Cache before writing
        df.cache()
        
        # Write partitioned by subreddit
        df.write \
          .mode("overwrite") \
          .partitionBy("subreddit") \
          .option("compression", "snappy") \
          .parquet(self.output_dir)

        # NEW: Also save to PostgreSQL
        print("\n🐘 Step 11b: Saving to PostgreSQL...")
        self.save_to_postgresql(df, "processed_reddit_posts")

        print(f"   ✅ Saved to: {self.output_dir}")
        
        # Show sample
        print("\n📋 Sample Processed Data:")
        df.select("post_id", "subreddit", "title_cleaned", "sentiment", 
                 "primary_topic", "entity_count", "score", "engagement_category") \
          .show(10, truncate=50)
        
        print("\n" + "="*70)
        print("✨ Processed Layer Complete!")
        print(f"📊 Total processed records: {df.count()}")
        
        # Unpersist
        df.unpersist()
        
        return df
    
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

    def save_to_postgresql(self, df, table_name="processed_reddit_posts"):
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
    processor = ProcessedLayerProcessorSpark(
        raw_data_path="data_spark/raw",
        output_dir="data_spark/processed"
    )

    try:
        df = processor.process()
        print("\n✅ Processed Layer Complete - Data ready for Analytics Layer")

        # Optionally save to PostgreSQL as well
        processor.save_to_postgresql(df, "processed_reddit_posts")

    finally:
        processor.stop()
