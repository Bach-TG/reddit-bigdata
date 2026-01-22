"""
ANALYTICS LAYER (GOLD) - PySpark Version
Mục đích: Tạo các bảng tổng hợp sẵn sàng cho visualization
Distributed aggregations with Spark SQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, max as spark_max, min as spark_min,
    stddev, round as spark_round, expr, when, collect_list, size,
    explode, row_number, dense_rank, lag, first, last, countDistinct
)
from pyspark.sql.window import Window
from datetime import datetime
import json
from db_connection_config import DB_CONFIG

# 12. **spark_03_analytics_layer.py** ⚡
#     - PySpark: Spark SQL Analytics
#     - Distributed aggregations
#     - Window functions for time series
#     - ~20 KB, 600 dòng
#     - **Chạy sau Processed Layer (PySpark)**

class AnalyticsLayerProcessorSpark:
    def __init__(self, processed_data_path="data_spark/processed", output_dir="data_spark/analytics"):
        self.processed_data_path = processed_data_path
        self.output_dir = output_dir
        
        # Initialize Spark Session with SQL optimizations
        self.spark = SparkSession.builder \
            .appName("RedditAnalyticsLayer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        print(f"✅ Spark Session created: {self.spark.version}")
    
    def load_processed_data(self):
        """Load processed data"""
        df = self.spark.read.parquet(self.processed_data_path)
        
        # Register as temp view for SQL queries
        df.createOrReplaceTempView("posts")
        
        record_count = df.count()
        print(f"📊 Loaded {record_count} processed records")
        
        return df
    
    # ========================================================================
    # TABLE 1: Daily Subreddit Statistics (Spark SQL)
    # ========================================================================
    def create_daily_subreddit_stats(self):
        """Aggregate daily statistics per subreddit using Spark SQL"""
        print("\n📊 Creating: Daily Subreddit Statistics (Spark SQL)...")
        
        query = """
        SELECT 
            subreddit,
            CAST(created_datetime AS DATE) as date,
            COUNT(*) as total_posts,
            SUM(score) as total_score,
            ROUND(AVG(score), 2) as avg_score,
            PERCENTILE_APPROX(score, 0.5) as median_score,
            MAX(score) as max_score,
            ROUND(STDDEV(score), 2) as score_std,
            ROUND(AVG(sentiment_polarity), 3) as avg_sentiment,
            ROUND(AVG(title_word_count), 1) as avg_title_length,
            ROUND(AVG(entity_count), 1) as avg_entity_count,
            SUM(CASE WHEN has_body THEN 1 ELSE 0 END) as posts_with_body
        FROM posts
        GROUP BY subreddit, CAST(created_datetime AS DATE)
        ORDER BY subreddit, date
        """
        
        daily_stats = self.spark.sql(query)
        
        # Add growth rate using window function
        window_spec = Window.partitionBy("subreddit").orderBy("date")
        daily_stats = daily_stats.withColumn(
            "posts_growth_rate",
            spark_round(
                (col("total_posts") - lag("total_posts").over(window_spec)) / 
                lag("total_posts").over(window_spec) * 100, 
                2
            )
        )
        
        # Save
        output_path = f"{self.output_dir}/daily_subreddit_stats"
        daily_stats.write.mode("overwrite").parquet(output_path)

        # NEW: Also save to PostgreSQL
        self.save_table_to_postgresql(daily_stats, "daily_subreddit_stats")

        print(f"   ✅ Saved: {output_path} ({daily_stats.count()} rows)")
        daily_stats.show(10, truncate=False)

        return daily_stats
    
    # ========================================================================
    # TABLE 2: Trending Topics (Spark SQL with Explode)
    # ========================================================================
    def create_trending_topics(self):
        """Analyze trending topics using array explosion"""
        print("\n🔥 Creating: Trending Topics Analysis (Spark SQL)...")
        
        # First explode topics array
        self.spark.sql("""
            SELECT 
                subreddit,
                CAST(created_datetime AS DATE) as date,
                exploded_topic as topic,
                score,
                sentiment_polarity,
                entity_count
            FROM posts
            LATERAL VIEW EXPLODE(topics) exploded_topics AS exploded_topic
        """).createOrReplaceTempView("topics_exploded")
        
        # Then aggregate
        query = """
        SELECT 
            date,
            topic,
            subreddit,
            COUNT(*) as mention_count,
            ROUND(AVG(score), 2) as avg_score,
            SUM(score) as total_score,
            ROUND(AVG(sentiment_polarity), 3) as avg_sentiment,
            ROUND(AVG(entity_count), 1) as avg_entities
        FROM topics_exploded
        GROUP BY date, topic, subreddit
        ORDER BY date DESC, mention_count DESC
        """
        
        topic_stats = self.spark.sql(query)
        
        # Calculate growth rate
        window_spec = Window.partitionBy("topic", "subreddit").orderBy("date")
        topic_stats = topic_stats.withColumn(
            "mention_growth_rate",
            spark_round(
                (col("mention_count") - lag("mention_count").over(window_spec)) /
                lag("mention_count").over(window_spec) * 100,
                2
            )
        )
        
        # Flag trending (high growth + volume)
        topic_stats = topic_stats.withColumn(
            "is_trending",
            (col("mention_growth_rate") > 50) & (col("mention_count") > 5)
        )
        
        # Save
        output_path = f"{self.output_dir}/trending_topics"
        topic_stats.write.mode("overwrite").parquet(output_path)

        # NEW: Also save to PostgreSQL
        self.save_table_to_postgresql(topic_stats, "trending_topics")

        print(f"   ✅ Saved: {output_path} ({topic_stats.count()} rows)")

        # Show trending
        trending = topic_stats.filter(col("is_trending")).orderBy(col("mention_growth_rate").desc())
        if trending.count() > 0:
            print("   🔥 Top trending topics:")
            trending.select("topic", "subreddit", "mention_count", "mention_growth_rate").show(5, truncate=False)

        return topic_stats
    
    # ========================================================================
    # TABLE 3: User Engagement Metrics
    # ========================================================================
    def create_user_engagement(self):
        """Analyze user posting patterns"""
        print("\n👥 Creating: User Engagement Metrics (Spark SQL)...")
        
        query = """
        SELECT 
            author,
            subreddit,
            CAST(created_datetime AS DATE) as date,
            COUNT(*) as post_count,
            SUM(score) as total_score,
            ROUND(AVG(score), 2) as avg_score,
            MAX(score) as max_score,
            ROUND(AVG(sentiment_polarity), 3) as avg_sentiment,
            FIRST(primary_topic) as most_common_topic,
            COLLECT_LIST(hour_of_day) as active_hours
        FROM posts
        GROUP BY author, subreddit, CAST(created_datetime AS DATE)
        ORDER BY post_count DESC
        """
        
        user_stats = self.spark.sql(query)
        
        # Calculate unique hours
        user_stats = user_stats.withColumn(
            "unique_hours",
            size(expr("array_distinct(active_hours)"))
        )
        
        # Power user flag
        user_stats = user_stats.withColumn(
            "is_power_user",
            col("post_count") > 5
        )
        
        # Save
        output_path = f"{self.output_dir}/user_engagement"
        user_stats.write.mode("overwrite").parquet(output_path)

        # NEW: Also save to PostgreSQL
        self.save_table_to_postgresql(user_stats, "user_engagement")

        print(f"   ✅ Saved: {output_path} ({user_stats.count()} rows)")

        # Top contributors
        print("   👑 Top contributors:")
        user_stats.orderBy(col("post_count").desc()).select(
            "author", "subreddit", "post_count", "avg_score"
        ).show(5, truncate=False)

        return user_stats
    
    # ========================================================================
    # TABLE 4: Cross-Subreddit Comparison (Pivot)
    # ========================================================================
    def create_cross_subreddit_comparison(self):
        """Compare topics across subreddits using pivot"""
        print("\n🔄 Creating: Cross-Subreddit Comparison (Spark SQL)...")
        
        # Explode topics first
        query = """
        SELECT 
            CAST(created_datetime AS DATE) as date,
            topic,
            subreddit,
            score,
            sentiment_polarity
        FROM (
            SELECT 
                created_datetime,
                explode(topics) as topic,
                subreddit,
                score,
                sentiment_polarity
            FROM posts
        )
        """
        
        df_exploded = self.spark.sql(query)
        
        # Create pivot for mentions
        mentions_pivot = df_exploded.groupBy("date", "topic") \
            .pivot("subreddit") \
            .agg(count("*")) \
            .selectExpr("date", "topic", *[
                f"`{c}` as `{c}_mentions`"
                for c in df_exploded.select("subreddit").distinct().rdd.flatMap(lambda x: x).collect()
            ])

        
        # Create pivot for avg_score
        score_pivot = df_exploded.groupBy("date", "topic").pivot("subreddit").agg(
            spark_round(avg("score"), 2).alias("avg_score")
        )
        
        # Create pivot for sentiment
        sentiment_pivot = df_exploded.groupBy("date", "topic").pivot("subreddit").agg(
            spark_round(avg("sentiment_polarity"), 3).alias("sentiment")
        )
        
        # # Join all pivots
        # comparison = mentions_pivot.join(score_pivot, ["date", "topic"], "left") \
        #                           .join(sentiment_pivot, ["date", "topic"], "left")
        
        # # Fill nulls with 0
        # comparison = comparison.fillna(0)
        comparison = df_exploded.groupBy("date", "topic") \
            .pivot("subreddit") \
            .agg(
                count("*").alias("mentions"),
                spark_round(avg("score"), 2).alias("avg_score"),
                spark_round(avg("sentiment_polarity"), 3).alias("sentiment")
            ) \
            .fillna(0)

        
        # Save
        output_path = f"{self.output_dir}/cross_subreddit_comparison"
        comparison.write.mode("overwrite").parquet(output_path)
        
        print(f"   ✅ Saved: {output_path} ({comparison.count()} rows)")
        comparison.show(5, truncate=False)
        
        return comparison
    
    # ========================================================================
    # TABLE 5: Hourly Patterns
    # ========================================================================
    def create_hourly_patterns(self):
        """Analyze posting patterns by hour"""
        print("\n⏰ Creating: Hourly Post Volume & Engagement...")
        
        query = """
        SELECT 
            subreddit,
            hour_of_day,
            COUNT(*) as post_count,
            ROUND(AVG(score), 2) as avg_score,
            SUM(score) as total_score,
            ROUND(AVG(sentiment_polarity), 3) as avg_sentiment,
            SUM(CASE WHEN engagement_category = 'viral' THEN 1 ELSE 0 END) as viral_posts
        FROM posts
        GROUP BY subreddit, hour_of_day
        ORDER BY subreddit, hour_of_day
        """
        
        hourly = self.spark.sql(query)
        
        # Peak hour flag
        window_spec = Window.partitionBy("subreddit")
        hourly = hourly.withColumn(
            "is_peak_hour",
            col("post_count") >= expr("percentile_approx(post_count, 0.75) OVER (PARTITION BY subreddit)")
        )
        
        # Save
        output_path = f"{self.output_dir}/hourly_patterns"
        hourly.write.mode("overwrite").parquet(output_path)

        # NEW: Also save to PostgreSQL
        self.save_table_to_postgresql(hourly, "hourly_patterns")

        print(f"   ✅ Saved: {output_path} ({hourly.count()} rows)")

        # Peak hours per subreddit
        print("   ⏰ Peak hours by subreddit:")
        hourly.filter(col("is_peak_hour")) \
              .groupBy("subreddit") \
              .agg(collect_list("hour_of_day").alias("peak_hours")) \
              .show(truncate=False)

        return hourly
    
    # ========================================================================
    # TABLE 6: Entity Co-occurrence Network
    # ========================================================================
    def create_entity_network(self):
        """Analyze entity relationships"""
        print("\n🌐 Creating: Entity Co-occurrence Network...")
        
        # This requires a self-join on entities
        query = """
        WITH entity_posts AS (
            SELECT 
                post_id,
                subreddit,
                explode(entities) as entity,
                sentiment,
                score
            FROM posts
            WHERE size(entities) >= 2
        )
        SELECT 
            a.entity as entity_1,
            b.entity as entity_2,
            COUNT(*) as co_occurrence_count,
            ROUND(AVG(a.score), 2) as avg_score,
            FIRST(a.sentiment) as dominant_sentiment
        FROM entity_posts a
        JOIN entity_posts b ON a.post_id = b.post_id AND a.entity < b.entity
        GROUP BY a.entity, b.entity
        HAVING COUNT(*) >= 2
        ORDER BY co_occurrence_count DESC
        """
        
        network = self.spark.sql(query)
        
        # Relationship strength
        network = network.withColumn(
            "relationship_strength",
            spark_round(col("co_occurrence_count") * col("avg_score"), 2)
        )
        
        if network.count() > 0:
            # Save
            output_path = f"{self.output_dir}/entity_network"
            network.write.mode("overwrite").parquet(output_path)

            # NEW: Also save to PostgreSQL
            self.save_table_to_postgresql(network, "entity_network")

            print(f"   ✅ Saved: {output_path} ({network.count()} rows)")

            print("   🔗 Top entity pairs:")
            network.orderBy(col("co_occurrence_count").desc()).show(5, truncate=False)
        else:
            print("   ⚠️  No entity pairs found")
            network = self.spark.createDataFrame([], "entity_1 string, entity_2 string")

        return network
    
    # ========================================================================
    # TABLE 7: Sentiment Time Series
    # ========================================================================
    def create_sentiment_timeseries(self):
        """Track sentiment over time"""
        print("\n📈 Creating: Sentiment Time Series...")
        
        query = """
        SELECT 
            subreddit,
            CAST(created_datetime AS DATE) as date,
            hour_of_day as hour,
            ROUND(AVG(sentiment_polarity), 3) as avg_polarity,
            ROUND(STDDEV(sentiment_polarity), 3) as polarity_std,
            COUNT(*) as post_count,
            ROUND(SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as positive_percentage
        FROM posts
        GROUP BY subreddit, CAST(created_datetime AS DATE), hour_of_day
        ORDER BY subreddit, date, hour
        """
        
        sentiment_ts = self.spark.sql(query)
        
        # Volatility flag
        sentiment_ts = sentiment_ts.withColumn(
            "is_volatile",
            col("polarity_std") > 0.3
        )
        
        # Save
        output_path = f"{self.output_dir}/sentiment_timeseries"
        sentiment_ts.write.mode("overwrite").parquet(output_path)

        # NEW: Also save to PostgreSQL
        self.save_table_to_postgresql(sentiment_ts, "sentiment_timeseries")

        print(f"   ✅ Saved: {output_path} ({sentiment_ts.count()} rows)")

        return sentiment_ts
    
    # ========================================================================
    # GENERATE INSIGHTS
    # ========================================================================
    def generate_insights(self, df):
        """Generate automated insights"""
        print("\n💡 Generating Insights...")
        
        # Collect key statistics
        total_posts = df.count()
        subreddits = [row.subreddit for row in df.select("subreddit").distinct().collect()]
        
        date_range = df.agg(
            spark_min("created_datetime").alias("min_date"),
            spark_max("created_datetime").alias("max_date")
        ).collect()[0]
        
        top_topics = df.groupBy("primary_topic").count().orderBy(col("count").desc()).limit(5).collect()
        
        insights = {
            'generated_at': datetime.now().isoformat(),
            'spark_version': self.spark.version,
            'data_summary': {
                'total_posts': total_posts,
                'subreddits': subreddits,
                'date_range': {
                    'start': str(date_range['min_date']),
                    'end': str(date_range['max_date'])
                },
                'top_topics': {row.primary_topic: row['count'] for row in top_topics}
            },
            'key_insights': []
        }
        
        # Insight 1: Most engaging topic
        topic_engagement = df.groupBy("primary_topic").agg(
            spark_round(avg("score"), 2).alias("avg_score")
        ).orderBy(col("avg_score").desc()).first()
        
        avg_overall = df.agg(avg("score")).collect()[0][0]
        
        insights['key_insights'].append({
            'type': 'engagement',
            'title': f'Highest Engagement: {topic_engagement.primary_topic}',
            'description': f'Posts about {topic_engagement.primary_topic} receive avg score of {topic_engagement.avg_score}, '
                          f'{(topic_engagement.avg_score / avg_overall - 1) * 100:.0f}% above average'
        })
        
        # Insight 2: Peak posting time
        peak_hour = df.groupBy("hour_of_day").count().orderBy(col("count").desc()).first()
        
        insights['key_insights'].append({
            'type': 'timing',
            'title': f'Peak Activity at {peak_hour.hour_of_day}:00',
            'description': f'Most posts created around {peak_hour.hour_of_day}:00 UTC ({peak_hour["count"]} posts)'
        })
        
        # Save insights
        output_path = f"{self.output_dir}/insights.json"
        with open(output_path, 'w') as f:
            json.dump(insights, f, indent=2)
        
        print(f"   ✅ Saved: {output_path}")
        
        # Display
        print("\n🎯 KEY INSIGHTS:")
        for i, insight in enumerate(insights['key_insights'], 1):
            print(f"\n{i}. {insight['title']}")
            print(f"   📊 {insight['description']}")
        
        return insights
    
    # ========================================================================
    # MAIN PROCESS
    # ========================================================================
    def process(self):
        """Execute all analytics"""
        print("="*70)
        print("🎯 Starting Analytics Layer Processing (PySpark)")
        print("="*70)
        
        # Load data
        print("\n📥 Loading processed data...")
        df = self.load_processed_data()
        
        # Cache for performance
        df.cache()
        
        # Create all tables
        tables = {}
        
        tables['daily_stats'] = self.create_daily_subreddit_stats()
        tables['trending_topics'] = self.create_trending_topics()
        tables['user_engagement'] = self.create_user_engagement()
        tables['cross_subreddit'] = self.create_cross_subreddit_comparison()
        tables['hourly_patterns'] = self.create_hourly_patterns()
        tables['entity_network'] = self.create_entity_network()
        tables['sentiment_ts'] = self.create_sentiment_timeseries()
        
        # Generate insights
        insights = self.generate_insights(df)
        
        # Unpersist
        df.unpersist()
        
        print("\n" + "="*70)
        print("✨ Analytics Layer Complete!")
        print(f"📊 Created {len(tables)} analytical tables")
        print(f"💡 Generated {len(insights['key_insights'])} key insights")
        print(f"📁 Output directory: {self.output_dir}")
        
        return tables, insights
    
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

    def save_table_to_postgresql(self, df, table_name):
        """Save a DataFrame to a PostgreSQL table"""
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
    processor = AnalyticsLayerProcessorSpark(
        processed_data_path="data_spark/processed",
        output_dir="data_spark/analytics"
    )

    try:
        tables, insights = processor.process()
        print("\n✅ Analytics Layer Complete - Ready for BI Tools & Advanced Analytics")

        # Optionally save to PostgreSQL as well
        for table_name, table_df in tables.items():
            processor.save_table_to_postgresql(table_df, table_name)

    finally:
        processor.stop()
