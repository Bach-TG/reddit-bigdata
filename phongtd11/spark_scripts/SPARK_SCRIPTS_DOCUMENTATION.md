# Spark Scripts Documentation

This documentation describes the three PySpark scripts that form a data processing pipeline for Reddit posts. The pipeline follows a three-layer architecture: Raw (Bronze), Processed (Silver), and Analytics (Gold).

## Script 1: spark_01_raw_layer.py

### Purpose
The Raw Layer (Bronze) script is responsible for collecting and storing raw data from the Reddit API. It's designed to be scalable to millions of records using PySpark for distributed processing.

### Input Fields
- **input_file**: Path to the input JSONL file containing Reddit posts data
  - Expected format: JSON Lines (JSONL) with the following structure:
    - `post_id`: String (required) - Unique identifier for the post
    - `subreddit`: String (required) - Name of the subreddit
    - `title`: String (required) - Title of the post
    - `body`: String (optional) - Body content of the post
    - `author`: String (required) - Author of the post
    - `created_utc`: Long integer (required) - Unix timestamp of creation
    - `score`: Integer (required) - Score/votes of the post

### Output Fields
The script produces the following outputs:

#### Parquet Files
Located in the output directory (`data_spark/raw` by default), partitioned by:
- `subreddit`: Subreddit name
- `partition_year`: Year extracted from creation timestamp
- `partition_month`: Month extracted from creation timestamp
- `partition_day`: Day extracted from creation timestamp

Each record contains:
- Original fields from input: `post_id`, `subreddit`, `title`, `body`, `author`, `created_utc`, `score`
- Metadata fields:
  - `ingestion_timestamp`: Timestamp when data was ingested
  - `batch_id`: Unique identifier for the ingestion batch
  - `source_api`: Source of the data ('reddit')
  - `data_quality_flag`: Quality indicator ('raw')
  - `row_hash`: MD5 hash of the row for deduplication
- Partition fields:
  - `created_datetime`: Converted datetime from Unix timestamp
  - `partition_date`: Date extracted from datetime
  - `partition_year`: Year for partitioning
  - `partition_month`: Month for partitioning
  - `partition_day`: Day for partitioning

#### Manifest File
- Location: `manifests/{batch_id}_manifest.json`
- Contains batch statistics including:
  - `batch_id`: Unique identifier for the batch
  - `ingestion_time`: Time of ingestion
  - `source_file`: Path to source file
  - `total_records`: Total number of records processed
  - `subreddits`: Dictionary mapping subreddit names to record counts
  - `date_range`: Min and max dates in the dataset
  - `schema_version`: Schema version
  - `spark_version`: Version of Spark used

### Key Features
- Distributed JSONL loading with Spark
- Automatic partitioning by date and subreddit
- Metadata addition for data lineage
- Row hashing for deduplication
- Batch manifest generation for tracking

## Script 2: spark_02_processed_layer.py

### Purpose
The Processed Layer (Silver) script performs data cleaning, normalization, and enrichment using Natural Language Processing (NLP) techniques. It applies distributed NLP processing with Spark UDFs for sentiment analysis, topic classification, and entity extraction.

### Input Fields
- **raw_data_path**: Path to the raw data directory (typically `data_spark/raw`)
  - Reads Parquet files containing records with fields from the raw layer:
    - `post_id`, `subreddit`, `title`, `body`, `author`, `created_utc`, `score`
    - Metadata fields: `ingestion_timestamp`, `batch_id`, `source_api`, `data_quality_flag`, `row_hash`
    - Partition fields: `created_datetime`, `partition_date`, `partition_year`, `partition_month`, `partition_day`

### Output Fields
The script produces enriched and processed data in Parquet format at the output directory (`data_spark/processed`), partitioned by subreddit. Each record contains:

#### Original Fields
- All fields from the raw layer

#### Text Processing Fields
- `title_cleaned`: Cleaned and normalized title text
- `body_cleaned`: Cleaned and normalized body text

#### NLP Enrichment Fields
- `sentiment_polarity`: Numerical sentiment score (-1 to 1)
- `sentiment`: Categorical sentiment label ('positive', 'negative', 'neutral')
- `sentiment_subjectivity`: Subjectivity score (simplified to 0.5)
- `topics`: Array of detected topics for the post
- `primary_topic`: Primary topic assigned to the post
- `entities`: Array of country entities extracted from the text
- `entity_count`: Number of entities found in the post

#### Text Feature Fields
- `title_word_count`: Number of words in the cleaned title
- `body_word_count`: Number of words in the cleaned body
- `has_body`: Boolean indicating if the post has body content
- `title_char_count`: Number of characters in the cleaned title

#### Time Feature Fields
- `hour_of_day`: Hour extracted from creation datetime
- `day_of_week`: Day of week (1-7) extracted from creation datetime
- `day_name`: Full name of the day
- `is_weekend`: Boolean indicating if the post was made on weekend
- `time_period`: Time period classification ('night', 'morning', 'afternoon', 'evening')

#### Engagement Metric Fields
- `score_normalized`: Normalized score (0-100 scale)
- `engagement_category`: Engagement level classification ('low', 'medium', 'high', 'viral')

### Key Features
- Distributed text cleaning and normalization
- Keyword-based sentiment analysis using UDFs
- Topic classification based on predefined keywords
- Country entity extraction
- Text feature calculation (word counts, character counts)
- Time-based feature engineering
- Engagement metric calculation and categorization
- Duplicate removal

## Script 3: spark_03_analytics_layer.py

### Purpose
The Analytics Layer (Gold) script creates aggregated tables ready for visualization and business intelligence. It performs distributed aggregations using Spark SQL and window functions for time series analysis.

### Input Fields
- **processed_data_path**: Path to the processed data directory (typically `data_spark/processed`)
  - Reads Parquet files containing records with all fields from the processed layer:
    - All original raw fields
    - Text processing fields: `title_cleaned`, `body_cleaned`
    - NLP enrichment fields: `sentiment_polarity`, `sentiment`, `topics`, `primary_topic`, `entities`, `entity_count`
    - Text feature fields: `title_word_count`, `body_word_count`, `has_body`, `title_char_count`
    - Time feature fields: `hour_of_day`, `day_of_week`, `day_name`, `is_weekend`, `time_period`
    - Engagement metric fields: `score_normalized`, `engagement_category`

### Output Tables
The script generates multiple analytical tables in Parquet format at the output directory (`data_spark/analytics`):

#### Table 1: Daily Subreddit Statistics
- Location: `analytics/daily_subreddit_stats`
- Fields:
  - `subreddit`: Subreddit name
  - `date`: Date of the posts
  - `total_posts`: Total number of posts for the day
  - `total_score`: Sum of scores for the day
  - `avg_score`: Average score for the day
  - `median_score`: Median score for the day
  - `max_score`: Maximum score for the day
  - `score_std`: Standard deviation of scores
  - `avg_sentiment`: Average sentiment polarity
  - `avg_title_length`: Average title length in words
  - `avg_entity_count`: Average number of entities mentioned
  - `posts_with_body`: Number of posts that have body content
  - `posts_growth_rate`: Growth rate compared to previous day

#### Table 2: Trending Topics
- Location: `analytics/trending_topics`
- Fields:
  - `date`: Date of the posts
  - `topic`: Topic name
  - `subreddit`: Subreddit name
  - `mention_count`: Number of times the topic was mentioned
  - `avg_score`: Average score of posts mentioning the topic
  - `total_score`: Total score of posts mentioning the topic
  - `avg_sentiment`: Average sentiment of posts mentioning the topic
  - `avg_entities`: Average number of entities in posts mentioning the topic
  - `mention_growth_rate`: Growth rate of mentions compared to previous day
  - `is_trending`: Boolean flag indicating if the topic is trending

#### Table 3: User Engagement Metrics
- Location: `analytics/user_engagement`
- Fields:
  - `author`: Author of the posts
  - `subreddit`: Subreddit where posts were made
  - `date`: Date of the posts
  - `post_count`: Number of posts by the user
  - `total_score`: Total score of user's posts
  - `avg_score`: Average score of user's posts
  - `max_score`: Maximum score of user's posts
  - `avg_sentiment`: Average sentiment of user's posts
  - `most_common_topic`: Most common topic in user's posts
  - `active_hours`: Array of hours when user is most active
  - `unique_hours`: Number of unique hours when user posts
  - `is_power_user`: Boolean flag indicating if user is a power user (>5 posts)

#### Table 4: Cross-Subreddit Comparison
- Location: `analytics/cross_subreddit_comparison`
- Fields:
  - `date`: Date of the posts
  - `topic`: Topic name
  - Various subreddit-specific fields for mentions, average scores, and sentiment

#### Table 5: Hourly Patterns
- Location: `analytics/hourly_patterns`
- Fields:
  - `subreddit`: Subreddit name
  - `hour_of_day`: Hour of the day (0-23)
  - `post_count`: Number of posts in that hour
  - `avg_score`: Average score of posts in that hour
  - `total_score`: Total score of posts in that hour
  - `avg_sentiment`: Average sentiment of posts in that hour
  - `viral_posts`: Number of viral posts in that hour
  - `is_peak_hour`: Boolean flag indicating if it's a peak hour for the subreddit

#### Table 6: Entity Co-occurrence Network
- Location: `analytics/entity_network`
- Fields:
  - `entity_1`: First entity in the relationship
  - `entity_2`: Second entity in the relationship
  - `co_occurrence_count`: Number of times entities appear together
  - `avg_score`: Average score of posts where entities co-occur
  - `dominant_sentiment`: Dominant sentiment in posts where entities co-occur
  - `relationship_strength`: Strength of the relationship (co-occurrence count × avg score)

#### Table 7: Sentiment Time Series
- Location: `analytics/sentiment_timeseries`
- Fields:
  - `subreddit`: Subreddit name
  - `date`: Date of the posts
  - `hour`: Hour of the day
  - `avg_polarity`: Average sentiment polarity
  - `polarity_std`: Standard deviation of sentiment polarity
  - `post_count`: Number of posts
  - `positive_percentage`: Percentage of positive sentiment posts
  - `is_volatile`: Boolean flag indicating if sentiment is volatile

#### Insights File
- Location: `analytics/insights.json`
- Contains automated insights including:
  - Summary statistics
  - Key findings about engagement, timing, and trends
  - Generated timestamp and Spark version

### Key Features
- Distributed aggregations using Spark SQL
- Window functions for time series analysis
- Array operations for topic and entity analysis
- Pivot operations for cross-subreddit comparison
- Automated insight generation
- Multiple analytical table creation
- Trend detection algorithms