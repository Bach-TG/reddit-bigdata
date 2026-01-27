-- ============================================================================
-- PostgreSQL Database Initialization for Reddit Pipeline
-- ============================================================================

-- Create main table for processed Reddit posts
CREATE TABLE IF NOT EXISTS reddit_posts (
    -- Primary identification
    post_id VARCHAR(50) PRIMARY KEY,

    -- Post metadata
    subreddit VARCHAR(100) NOT NULL,
    author VARCHAR(100) NOT NULL,
    created_utc BIGINT NOT NULL,
    created_datetime TIMESTAMP,
    score INTEGER NOT NULL,

    -- Original content
    title TEXT NOT NULL,
    body TEXT,

    -- Cleaned content
    title_cleaned TEXT,
    body_cleaned TEXT,

    -- NLP Analysis
    sentiment_score FLOAT,
    sentiment VARCHAR(20),
    topic VARCHAR(50),

    -- Entity extraction (stored as JSON string)
    entities_str TEXT,
    entity_count INTEGER,

    -- Text features
    title_word_count INTEGER,
    has_body BOOLEAN,

    -- Engagement metrics
    engagement_category VARCHAR(20),

    -- Pipeline timestamps
    kafka_ingestion_time TIMESTAMP,
    processing_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table for raw Reddit posts
CREATE TABLE IF NOT EXISTS raw_reddit_posts (
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

-- Create table for processed Reddit posts
CREATE TABLE IF NOT EXISTS processed_reddit_posts (
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

-- Create table for daily subreddit statistics
CREATE TABLE IF NOT EXISTS daily_subreddit_stats (
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

-- Create table for trending topics
CREATE TABLE IF NOT EXISTS trending_topics (
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

-- Create table for user engagement metrics
CREATE TABLE IF NOT EXISTS user_engagement (
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

-- Create table for hourly patterns
CREATE TABLE IF NOT EXISTS hourly_patterns (
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

-- Create table for entity co-occurrence network
CREATE TABLE IF NOT EXISTS entity_network (
    entity_1 VARCHAR(255),
    entity_2 VARCHAR(255),
    co_occurrence_count INTEGER,
    avg_score DECIMAL(10,2),
    dominant_sentiment VARCHAR(20),
    relationship_strength DECIMAL(10,2),
    PRIMARY KEY (entity_1, entity_2)
);

-- Create table for sentiment time series
CREATE TABLE IF NOT EXISTS sentiment_timeseries (
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

-- ============================================================================
-- INDEXES for Query Performance
-- ============================================================================

-- Most common queries filter by subreddit
CREATE INDEX IF NOT EXISTS idx_subreddit ON reddit_posts(subreddit);

-- Time-series queries
CREATE INDEX IF NOT EXISTS idx_created_datetime ON reddit_posts(created_datetime);
CREATE INDEX IF NOT EXISTS idx_processing_timestamp ON reddit_posts(processing_timestamp);

-- Analytics queries
CREATE INDEX IF NOT EXISTS idx_topic ON reddit_posts(topic);
CREATE INDEX IF NOT EXISTS idx_sentiment ON reddit_posts(sentiment);
CREATE INDEX IF NOT EXISTS idx_engagement ON reddit_posts(engagement_category);
CREATE INDEX IF NOT EXISTS idx_score ON reddit_posts(score DESC);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_subreddit_date ON reddit_posts(subreddit, created_datetime);
CREATE INDEX IF NOT EXISTS idx_topic_sentiment ON reddit_posts(topic, sentiment);

-- Full-text search on titles (optional, for search features)
CREATE INDEX IF NOT EXISTS idx_title_fts ON reddit_posts USING GIN(to_tsvector('english', title));

-- ============================================================================
-- MATERIALIZED VIEWS for Analytics
-- ============================================================================

-- Daily statistics per subreddit
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_subreddit_stats AS
SELECT 
    subreddit,
    DATE(created_datetime) as date,
    COUNT(*) as total_posts,
    SUM(score) as total_score,
    AVG(score) as avg_score,
    MAX(score) as max_score,
    STDDEV(score) as score_stddev,
    AVG(sentiment_score) as avg_sentiment,
    AVG(title_word_count) as avg_title_length,
    COUNT(CASE WHEN has_body THEN 1 END) as posts_with_body,
    COUNT(DISTINCT author) as unique_authors
FROM reddit_posts
GROUP BY subreddit, DATE(created_datetime);

CREATE INDEX IF NOT EXISTS idx_mv_daily_subreddit ON mv_daily_subreddit_stats(subreddit, date);

-- Hourly sentiment trends
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_sentiment AS
SELECT 
    DATE_TRUNC('hour', created_datetime) as hour,
    subreddit,
    sentiment,
    COUNT(*) as count,
    AVG(sentiment_score) as avg_score,
    AVG(score) as avg_engagement
FROM reddit_posts
GROUP BY DATE_TRUNC('hour', created_datetime), subreddit, sentiment;

CREATE INDEX IF NOT EXISTS idx_mv_hourly_sentiment ON mv_hourly_sentiment(hour, subreddit);

-- Topic performance
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_topic_performance AS
SELECT 
    topic,
    subreddit,
    COUNT(*) as mention_count,
    AVG(score) as avg_score,
    AVG(sentiment_score) as avg_sentiment,
    COUNT(DISTINCT author) as unique_authors,
    MAX(created_datetime) as last_mention
FROM reddit_posts
GROUP BY topic, subreddit;

CREATE INDEX IF NOT EXISTS idx_mv_topic_performance ON mv_topic_performance(topic, subreddit);

-- ============================================================================
-- FUNCTIONS for Data Quality
-- ============================================================================

-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_subreddit_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_sentiment;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_topic_performance;
END;
$$ LANGUAGE plpgsql;

-- Function to get data quality metrics
CREATE OR REPLACE FUNCTION get_data_quality_metrics(hours_back INTEGER DEFAULT 24)
RETURNS TABLE (
    metric_name VARCHAR,
    metric_value NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'total_records'::VARCHAR as metric_name,
        COUNT(*)::NUMERIC as metric_value
    FROM reddit_posts
    WHERE processing_timestamp >= NOW() - (hours_back || ' hours')::INTERVAL
    
    UNION ALL
    
    SELECT 
        'null_titles'::VARCHAR,
        COUNT(CASE WHEN title_cleaned IS NULL THEN 1 END)::NUMERIC
    FROM reddit_posts
    WHERE processing_timestamp >= NOW() - (hours_back || ' hours')::INTERVAL
    
    UNION ALL
    
    SELECT 
        'null_sentiment'::VARCHAR,
        COUNT(CASE WHEN sentiment IS NULL THEN 1 END)::NUMERIC
    FROM reddit_posts
    WHERE processing_timestamp >= NOW() - (hours_back || ' hours')::INTERVAL
    
    UNION ALL
    
    SELECT 
        'null_topic'::VARCHAR,
        COUNT(CASE WHEN topic IS NULL THEN 1 END)::NUMERIC
    FROM reddit_posts
    WHERE processing_timestamp >= NOW() - (hours_back || ' hours')::INTERVAL
    
    UNION ALL
    
    SELECT 
        'avg_processing_delay_seconds'::VARCHAR,
        AVG(EXTRACT(EPOCH FROM (processing_timestamp - kafka_ingestion_time)))::NUMERIC
    FROM reddit_posts
    WHERE processing_timestamp >= NOW() - (hours_back || ' hours')::INTERVAL;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- AGGREGATION TABLES for Fast Queries
-- ============================================================================

-- Pre-aggregated subreddit stats (updated by trigger or scheduled job)
CREATE TABLE IF NOT EXISTS agg_subreddit_stats (
    subreddit VARCHAR(100) PRIMARY KEY,
    total_posts BIGINT,
    total_score BIGINT,
    avg_score NUMERIC(10, 2),
    max_score INTEGER,
    unique_authors INTEGER,
    last_post_time TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pre-aggregated topic stats
CREATE TABLE IF NOT EXISTS agg_topic_stats (
    topic VARCHAR(50) PRIMARY KEY,
    total_mentions BIGINT,
    avg_score NUMERIC(10, 2),
    avg_sentiment NUMERIC(5, 3),
    trending_score NUMERIC(10, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- PARTITIONING (Optional, for very large datasets)
-- ============================================================================

-- Example: Partition by month for time-series data
-- CREATE TABLE reddit_posts_partitioned (
--     LIKE reddit_posts INCLUDING ALL
-- ) PARTITION BY RANGE (created_datetime);
-- 
-- CREATE TABLE reddit_posts_2024_01 PARTITION OF reddit_posts_partitioned
--     FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
-- 
-- CREATE TABLE reddit_posts_2024_02 PARTITION OF reddit_posts_partitioned
--     FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- ============================================================================
-- MONITORING & ALERTS
-- ============================================================================

-- Table to store processing metrics
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_time ON pipeline_metrics(metric_timestamp);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_name ON pipeline_metrics(metric_name);

-- ============================================================================
-- INITIAL DATA VALIDATION
-- ============================================================================

-- Verify table structure
SELECT 
    'reddit_posts' as table_name,
    COUNT(*) as column_count
FROM information_schema.columns
WHERE table_name = 'reddit_posts';

-- Verify indexes
SELECT 
    schemaname,
    tablename,
    indexname
FROM pg_indexes
WHERE tablename = 'reddit_posts';

-- Log initialization
INSERT INTO pipeline_metrics (metric_name, metric_value, metadata)
VALUES ('database_initialized', 1, '{"timestamp": "' || NOW() || '"}');

-- ============================================================================
-- GRANTS & PERMISSIONS
-- ============================================================================

-- Grant permissions to application user
GRANT SELECT, INSERT, UPDATE ON reddit_posts TO reddit_user;
GRANT SELECT, INSERT, UPDATE ON raw_reddit_posts TO reddit_user;
GRANT SELECT, INSERT, UPDATE ON processed_reddit_posts TO reddit_user;
GRANT SELECT ON mv_daily_subreddit_stats TO reddit_user;
GRANT SELECT ON mv_hourly_sentiment TO reddit_user;
GRANT SELECT ON mv_topic_performance TO reddit_user;
GRANT SELECT ON daily_subreddit_stats TO reddit_user;
GRANT SELECT ON trending_topics TO reddit_user;
GRANT SELECT ON user_engagement TO reddit_user;
GRANT SELECT ON hourly_patterns TO reddit_user;
GRANT SELECT ON entity_network TO reddit_user;
GRANT SELECT ON sentiment_timeseries TO reddit_user;
GRANT SELECT, INSERT ON pipeline_metrics TO reddit_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO reddit_user;

-- ============================================================================
-- MAINTENANCE
-- ============================================================================

-- Auto-vacuum settings for high-write table
ALTER TABLE reddit_posts SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ Database initialization complete!';
    RAISE NOTICE '📊 Tables: reddit_posts, raw_reddit_posts, processed_reddit_posts created';
    RAISE NOTICE '📊 Analytics tables: daily_subreddit_stats, trending_topics, user_engagement, hourly_patterns, entity_network, sentiment_timeseries created';
    RAISE NOTICE '📈 Indexes: 10 indexes created';
    RAISE NOTICE '🔍 Materialized Views: 3 views created';
    RAISE NOTICE '⚡ Functions: 2 functions created';
END $$;
