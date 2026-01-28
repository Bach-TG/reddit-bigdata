"""
PostgreSQL Table Creation Script for Reddit Visualization Dashboard
Based on: phongtd11/postgres_scripts/init_postgres.sql + BigData/scripts/create_table.py
Compatible with Spark pipeline output
"""

import os
import psycopg2
from psycopg2 import sql
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration from environment variables
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'reddit_db'),
    'user': os.getenv('POSTGRES_USER', 'reddit_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'reddit_pass')
}


def get_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        logger.error(f"Connection error: {e}")
        raise


def create_database_if_not_exists():
    """Create database if it doesn't exist"""
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
            (DB_CONFIG['database'],)
        )
        
        if not cursor.fetchone():
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(DB_CONFIG['database'])
            ))
            logger.info(f"✅ Created database: {DB_CONFIG['database']}")
        else:
            logger.info(f"📌 Database already exists: {DB_CONFIG['database']}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not create database: {e}")


# ============================================================================
# TABLE DEFINITIONS
# ============================================================================

TABLES = {
    # Main table for posts with NLP analysis
    'reddit_posts': """
        CREATE TABLE IF NOT EXISTS reddit_posts (
            -- Primary identification
            post_id VARCHAR(50) PRIMARY KEY,
            
            -- Post metadata
            subreddit VARCHAR(100) NOT NULL,
            author VARCHAR(100),
            created_utc BIGINT NOT NULL,
            created_datetime TIMESTAMP,
            score INTEGER DEFAULT 0,
            
            -- Original content
            title TEXT NOT NULL,
            body TEXT,
            
            -- Cleaned content (for NLP)
            title_cleaned TEXT,
            body_cleaned TEXT,
            
            -- Comments data
            comment_count INTEGER DEFAULT 0,
            comments_text TEXT,  -- Concatenated comments for analysis
            
            -- NLP Analysis
            sentiment_score FLOAT,
            sentiment VARCHAR(20),  -- positive, negative, neutral
            topic VARCHAR(50),      -- war_conflict, politics, trade_economy, policy, other
            
            -- Entity extraction (stored as JSON string)
            entities TEXT,  -- JSON array of countries/entities
            entity_count INTEGER DEFAULT 0,
            
            -- Keywords
            keywords TEXT,  -- JSON array of keywords
            
            -- Text features
            title_word_count INTEGER,
            body_word_count INTEGER,
            has_body BOOLEAN DEFAULT FALSE,
            
            -- Engagement metrics
            engagement_category VARCHAR(20),  -- low, medium, high, viral
            
            -- Time features
            hour_of_day INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR(20),
            is_weekend BOOLEAN,
            
            -- Pipeline timestamps
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processing_timestamp TIMESTAMP
        )
    """,
    
    # Keywords extracted from posts
    'post_keywords': """
        CREATE TABLE IF NOT EXISTS post_keywords (
            id SERIAL PRIMARY KEY,
            post_id VARCHAR(50) REFERENCES reddit_posts(post_id) ON DELETE CASCADE,
            keyword VARCHAR(100) NOT NULL,
            frequency INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """,
    
    # Keywords trend over time
    'keywords_trend': """
        CREATE TABLE IF NOT EXISTS keywords_trend (
            id SERIAL PRIMARY KEY,
            keyword VARCHAR(100) NOT NULL,
            date DATE NOT NULL,
            count INTEGER DEFAULT 0,
            avg_score FLOAT,
            avg_sentiment FLOAT,
            subreddit VARCHAR(100),
            UNIQUE(keyword, date, subreddit)
        )
    """,
    
    # Sentiment statistics aggregated
    'sentiment_stats': """
        CREATE TABLE IF NOT EXISTS sentiment_stats (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            subreddit VARCHAR(100),
            sentiment VARCHAR(20) NOT NULL,
            count INTEGER DEFAULT 0,
            avg_score FLOAT,
            UNIQUE(date, subreddit, sentiment)
        )
    """,
    
    # Subreddit comparison statistics
    'subreddit_stats': """
        CREATE TABLE IF NOT EXISTS subreddit_stats (
            id SERIAL PRIMARY KEY,
            subreddit VARCHAR(100) NOT NULL,
            date DATE NOT NULL,
            total_posts INTEGER DEFAULT 0,
            total_score BIGINT DEFAULT 0,
            avg_score FLOAT,
            max_score INTEGER,
            avg_sentiment FLOAT,
            avg_title_length FLOAT,
            posts_with_body INTEGER DEFAULT 0,
            unique_authors INTEGER DEFAULT 0,
            UNIQUE(subreddit, date)
        )
    """,
    
    # Topic trending analysis
    'trending_topics': """
        CREATE TABLE IF NOT EXISTS trending_topics (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            topic VARCHAR(100) NOT NULL,
            subreddit VARCHAR(100),
            mention_count INTEGER DEFAULT 0,
            avg_score FLOAT,
            total_score BIGINT,
            avg_sentiment FLOAT,
            is_trending BOOLEAN DEFAULT FALSE,
            UNIQUE(date, topic, subreddit)
        )
    """,
    
    # Hourly posting patterns
    'hourly_patterns': """
        CREATE TABLE IF NOT EXISTS hourly_patterns (
            id SERIAL PRIMARY KEY,
            subreddit VARCHAR(100) NOT NULL,
            hour_of_day INTEGER NOT NULL,
            post_count INTEGER DEFAULT 0,
            avg_score FLOAT,
            total_score BIGINT,
            avg_sentiment FLOAT,
            is_peak_hour BOOLEAN DEFAULT FALSE,
            UNIQUE(subreddit, hour_of_day)
        )
    """,
    
    # Entity co-occurrence network
    'entity_network': """
        CREATE TABLE IF NOT EXISTS entity_network (
            id SERIAL PRIMARY KEY,
            entity_1 VARCHAR(100) NOT NULL,
            entity_2 VARCHAR(100) NOT NULL,
            co_occurrence_count INTEGER DEFAULT 0,
            avg_score FLOAT,
            dominant_sentiment VARCHAR(20),
            UNIQUE(entity_1, entity_2)
        )
    """
}


# ============================================================================
# INDEXES for Query Performance
# ============================================================================

INDEXES = [
    # reddit_posts indexes
    "CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON reddit_posts(subreddit)",
    "CREATE INDEX IF NOT EXISTS idx_posts_datetime ON reddit_posts(created_datetime)",
    "CREATE INDEX IF NOT EXISTS idx_posts_topic ON reddit_posts(topic)",
    "CREATE INDEX IF NOT EXISTS idx_posts_sentiment ON reddit_posts(sentiment)",
    "CREATE INDEX IF NOT EXISTS idx_posts_engagement ON reddit_posts(engagement_category)",
    "CREATE INDEX IF NOT EXISTS idx_posts_score ON reddit_posts(score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_posts_subreddit_date ON reddit_posts(subreddit, created_datetime)",
    
    # post_keywords indexes
    "CREATE INDEX IF NOT EXISTS idx_keywords_post ON post_keywords(post_id)",
    "CREATE INDEX IF NOT EXISTS idx_keywords_keyword ON post_keywords(keyword)",
    
    # keywords_trend indexes
    "CREATE INDEX IF NOT EXISTS idx_trend_date ON keywords_trend(date)",
    "CREATE INDEX IF NOT EXISTS idx_trend_keyword ON keywords_trend(keyword)",
    
    # sentiment_stats indexes
    "CREATE INDEX IF NOT EXISTS idx_sentiment_date ON sentiment_stats(date)",
    "CREATE INDEX IF NOT EXISTS idx_sentiment_subreddit ON sentiment_stats(subreddit)",
    
    # subreddit_stats indexes
    "CREATE INDEX IF NOT EXISTS idx_subreddit_stats_date ON subreddit_stats(date)",
    "CREATE INDEX IF NOT EXISTS idx_subreddit_stats_sub ON subreddit_stats(subreddit)",
]


def create_tables(conn):
    """Create all tables"""
    cursor = conn.cursor()
    
    for table_name, create_sql in TABLES.items():
        try:
            cursor.execute(create_sql)
            logger.info(f"✅ Created table: {table_name}")
        except Exception as e:
            logger.error(f"❌ Error creating {table_name}: {e}")
            raise
    
    conn.commit()
    cursor.close()


def create_indexes(conn):
    """Create all indexes"""
    cursor = conn.cursor()
    
    for index_sql in INDEXES:
        try:
            cursor.execute(index_sql)
        except Exception as e:
            logger.warning(f"⚠️ Index warning: {e}")
    
    conn.commit()
    cursor.close()
    logger.info(f"✅ Created {len(INDEXES)} indexes")


def verify_tables(conn):
    """Verify all tables exist"""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    logger.info(f"📊 Tables in database: {tables}")
    return tables


def drop_all_tables(conn):
    """Drop all tables (use with caution!)"""
    cursor = conn.cursor()
    
    # Drop in reverse order due to foreign keys
    table_order = ['post_keywords', 'keywords_trend', 'sentiment_stats', 
                   'subreddit_stats', 'trending_topics', 'hourly_patterns',
                   'entity_network', 'reddit_posts']
    
    for table_name in table_order:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            logger.info(f"🗑️ Dropped table: {table_name}")
        except Exception as e:
            logger.warning(f"⚠️ Could not drop {table_name}: {e}")
    
    conn.commit()
    cursor.close()


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Create PostgreSQL tables for Reddit Visualization')
    parser.add_argument('--drop', action='store_true', help='Drop existing tables before creating')
    parser.add_argument('--verify', action='store_true', help='Only verify tables exist')
    args = parser.parse_args()
    
    print("=" * 70)
    print("🗄️  PostgreSQL Table Creation for Reddit Visualization")
    print("=" * 70)
    print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"Database: {DB_CONFIG['database']}")
    print(f"User: {DB_CONFIG['user']}")
    print("=" * 70)
    
    # Create database if not exists
    create_database_if_not_exists()
    
    # Connect to database
    conn = get_connection()
    
    try:
        if args.verify:
            tables = verify_tables(conn)
            print(f"\n✅ Found {len(tables)} tables")
            return
        
        if args.drop:
            print("\n⚠️ Dropping existing tables...")
            drop_all_tables(conn)
        
        print("\n📦 Creating tables...")
        create_tables(conn)
        
        print("\n📈 Creating indexes...")
        create_indexes(conn)
        
        print("\n🔍 Verifying tables...")
        tables = verify_tables(conn)
        
        print("\n" + "=" * 70)
        print(f"✨ Setup complete! Created {len(TABLES)} tables with {len(INDEXES)} indexes")
        print("=" * 70)
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
