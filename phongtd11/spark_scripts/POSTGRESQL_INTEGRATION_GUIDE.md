# PostgreSQL Integration Guide for Spark Scripts

This guide explains how to extend the existing PySpark scripts to write their output data to PostgreSQL tables.

## Prerequisites

Before integrating with PostgreSQL, you'll need to:

1. Install the PostgreSQL JDBC driver:
```bash
# Download the PostgreSQL JDBC driver jar file
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
```

2. Install required Python packages:
```bash
pip install psycopg2-binary sqlalchemy
```

3. Configure your PostgreSQL connection details:
```python
# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'your_database_name',
    'username': 'your_username',
    'password': 'your_password'
}
```

## Method 1: Using Spark's Built-in JDBC Connector

The most efficient way to write Spark DataFrames to PostgreSQL is using Spark's built-in JDBC connector.

### Connection String Format
```python
def get_jdbc_url():
    return f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def get_properties():
    properties = {
        "user": DB_CONFIG['username'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }
    return properties
```

## Method 2: Writing Each Script's Output to PostgreSQL

### 1. Writing spark_01_raw_layer.py Output to PostgreSQL

After the `save_to_parquet()` method in the `RawLayerProcessorSpark` class, add a new method:

```python
def save_to_postgresql(self, df, table_name="raw_reddit_posts"):
    """Save DataFrame to PostgreSQL table"""
    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    properties = {
        "user": DB_CONFIG['username'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }
    
    # Write to PostgreSQL
    df.write \
      .mode("overwrite") \
      .jdbc(url=jdbc_url, table=table_name, properties=properties)
    
    print(f"✅ Data saved to PostgreSQL table: {table_name}")

# Modify the process method to include PostgreSQL saving
def process(self):
    # ... existing code ...
    
    # Step 4: Save to Parquet
    print("\n💾 Step 4: Saving to Parquet format...")
    self.save_to_parquet(df)
    
    # Step 4b: Save to PostgreSQL
    print("\n🐘 Step 4b: Saving to PostgreSQL...")
    self.save_to_postgresql(df, "raw_reddit_posts")
    
    # ... rest of existing code ...
```

**PostgreSQL Table Schema for Raw Layer:**
```sql
CREATE TABLE raw_reddit_posts (
    post_id VARCHAR(255) PRIMARY KEY,
    subreddit VARCHAR(255),
    title TEXT,
    body TEXT,
    author VARCHAR(255),
    created_utc BIGINT,
    score INTEGER,
    ingestion_timestamp TIMESTAMP,
    batch_id VARCHAR(255),
    source_api VARCHAR(100),
    data_quality_flag VARCHAR(50),
    row_hash VARCHAR(255),
    created_datetime TIMESTAMP,
    partition_date DATE,
    partition_year INTEGER,
    partition_month INTEGER,
    partition_day INTEGER
);
```

### 2. Writing spark_02_processed_layer.py Output to PostgreSQL

Add a similar method to the `ProcessedLayerProcessorSpark` class:

```python
def save_to_postgresql(self, df, table_name="processed_reddit_posts"):
    """Save DataFrame to PostgreSQL table"""
    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    properties = {
        "user": DB_CONFIG['username'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }
    
    # Write to PostgreSQL
    df.write \
      .mode("overwrite") \
      .jdbc(url=jdbc_url, table=table_name, properties=properties)
    
    print(f"✅ Data saved to PostgreSQL table: {table_name}")

# Modify the process method to include PostgreSQL saving
def process(self):
    # ... existing code ...
    
    # Step 11: Save processed data to Parquet
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
    
    # ... rest of existing code ...
```

**PostgreSQL Table Schema for Processed Layer:**
```sql
CREATE TABLE processed_reddit_posts (
    post_id VARCHAR(255) PRIMARY KEY,
    subreddit VARCHAR(255),
    title TEXT,
    body TEXT,
    author VARCHAR(255),
    created_utc BIGINT,
    score INTEGER,
    ingestion_timestamp TIMESTAMP,
    batch_id VARCHAR(255),
    source_api VARCHAR(100),
    data_quality_flag VARCHAR(50),
    row_hash VARCHAR(255),
    created_datetime TIMESTAMP,
    partition_date DATE,
    partition_year INTEGER,
    partition_month INTEGER,
    partition_day INTEGER,
    title_cleaned TEXT,
    body_cleaned TEXT,
    sentiment_polarity DECIMAL(5,4),
    sentiment VARCHAR(20),
    sentiment_subjectivity DECIMAL(5,4),
    topics TEXT[], -- Requires PostgreSQL array support
    primary_topic VARCHAR(100),
    entities TEXT[], -- Requires PostgreSQL array support
    entity_count INTEGER,
    title_word_count INTEGER,
    body_word_count INTEGER,
    has_body BOOLEAN,
    title_char_count INTEGER,
    hour_of_day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    time_period VARCHAR(20),
    score_normalized DECIMAL(8,2),
    engagement_category VARCHAR(20)
);
```

### 3. Writing spark_03_analytics_layer.py Output to PostgreSQL

For the analytics layer, you'll need to write each analytical table to separate PostgreSQL tables:

```python
def save_table_to_postgresql(self, df, table_name):
    """Save a DataFrame to a PostgreSQL table"""
    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    properties = {
        "user": DB_CONFIG['username'],
        "password": DB_CONFIG['password'],
        "driver": "org.postgresql.Driver"
    }
    
    # Write to PostgreSQL
    df.write \
      .mode("overwrite") \
      .jdbc(url=jdbc_url, table=table_name, properties=properties)
    
    print(f"✅ Data saved to PostgreSQL table: {table_name}")

# Modify each table creation method to also save to PostgreSQL
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

    # Save to Parquet
    output_path = f"{self.output_dir}/daily_subreddit_stats"
    daily_stats.write.mode("overwrite").parquet(output_path)

    # NEW: Also save to PostgreSQL
    self.save_table_to_postgresql(daily_stats, "daily_subreddit_stats")

    print(f"   ✅ Saved: {output_path} ({daily_stats.count()} rows)")
    daily_stats.show(10, truncate=False)

    return daily_stats

# Apply the same pattern to other table creation methods...
```

**PostgreSQL Table Schemas for Analytics Layer:**

```sql
-- Table 1: Daily Subreddit Statistics
CREATE TABLE daily_subreddit_stats (
    subreddit VARCHAR(255),
    date DATE,
    total_posts INTEGER,
    total_score BIGINT,
    avg_score DECIMAL(10,2),
    median_score INTEGER,
    max_score INTEGER,
    score_std DECIMAL(10,2),
    avg_sentiment DECIMAL(10,3),
    avg_title_length DECIMAL(10,1),
    avg_entity_count DECIMAL(10,1),
    posts_with_body INTEGER,
    posts_growth_rate DECIMAL(10,2),
    PRIMARY KEY (subreddit, date)
);

-- Table 2: Trending Topics
CREATE TABLE trending_topics (
    date DATE,
    topic VARCHAR(255),
    subreddit VARCHAR(255),
    mention_count INTEGER,
    avg_score DECIMAL(10,2),
    total_score BIGINT,
    avg_sentiment DECIMAL(10,3),
    avg_entities DECIMAL(10,1),
    mention_growth_rate DECIMAL(10,2),
    is_trending BOOLEAN,
    PRIMARY KEY (date, topic, subreddit)
);

-- Table 3: User Engagement Metrics
CREATE TABLE user_engagement (
    author VARCHAR(255),
    subreddit VARCHAR(255),
    date DATE,
    post_count INTEGER,
    total_score BIGINT,
    avg_score DECIMAL(10,2),
    max_score INTEGER,
    avg_sentiment DECIMAL(10,3),
    most_common_topic VARCHAR(255),
    active_hours INTEGER[],
    unique_hours INTEGER,
    is_power_user BOOLEAN,
    PRIMARY KEY (author, subreddit, date)
);

-- Table 5: Hourly Patterns
CREATE TABLE hourly_patterns (
    subreddit VARCHAR(255),
    hour_of_day INTEGER,
    post_count INTEGER,
    avg_score DECIMAL(10,2),
    total_score BIGINT,
    avg_sentiment DECIMAL(10,3),
    viral_posts INTEGER,
    is_peak_hour BOOLEAN,
    PRIMARY KEY (subreddit, hour_of_day)
);

-- Table 6: Entity Co-occurrence Network
CREATE TABLE entity_network (
    entity_1 VARCHAR(255),
    entity_2 VARCHAR(255),
    co_occurrence_count INTEGER,
    avg_score DECIMAL(10,2),
    dominant_sentiment VARCHAR(20),
    relationship_strength DECIMAL(10,2),
    PRIMARY KEY (entity_1, entity_2)
);

-- Table 7: Sentiment Time Series
CREATE TABLE sentiment_timeseries (
    subreddit VARCHAR(255),
    date DATE,
    hour INTEGER,
    avg_polarity DECIMAL(10,3),
    polarity_std DECIMAL(10,3),
    post_count INTEGER,
    positive_percentage DECIMAL(10,1),
    is_volatile BOOLEAN,
    PRIMARY KEY (subreddit, date, hour)
);
```

## Complete Integration Example

Here's how to modify the main execution section of each script to include PostgreSQL output:

**For spark_01_raw_layer.py:**
```python
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
```

## Performance Considerations

1. **Batch Size**: For large datasets, consider using appropriate batch sizes:
```python
properties = {
    "user": DB_CONFIG['username'],
    "password": DB_CONFIG['password'],
    "driver": "org.postgresql.Driver",
    "batchsize": "10000"  # Adjust based on your data size
}
```

2. **Partitioning**: For very large datasets, consider writing in partitions:
```python
# Write by subreddit partitions to PostgreSQL
subreddits = df.select("subreddit").distinct().collect()
for row in subreddits:
    subreddit_name = row['subreddit']
    filtered_df = df.filter(col("subreddit") == subreddit_name)
    table_name = f"raw_reddit_posts_{subreddit_name.replace('-', '_')}"
    filtered_df.write.mode("overwrite").jdbc(url=jdbc_url, table=table_name, properties=properties)
```

3. **Connection Pooling**: For production environments, consider using connection pooling and monitoring.

## Error Handling

Always wrap database operations in try-catch blocks:
```python
def save_to_postgresql_with_retry(self, df, table_name, max_retries=3):
    """Save DataFrame to PostgreSQL with retry logic"""
    for attempt in range(max_retries):
        try:
            jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            
            properties = {
                "user": DB_CONFIG['username'],
                "password": DB_CONFIG['password'],
                "driver": "org.postgresql.Driver"
            }
            
            df.write \
              .mode("overwrite") \
              .jdbc(url=jdbc_url, table=table_name, properties=properties)
            
            print(f"✅ Successfully saved to PostgreSQL table: {table_name}")
            return
            
        except Exception as e:
            print(f"❌ Attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                raise e
            time.sleep(2 ** attempt)  # Exponential backoff
```

This guide provides a comprehensive approach to integrating your Spark scripts with PostgreSQL, allowing you to store processed data in a relational database for further analysis, reporting, or serving to applications.