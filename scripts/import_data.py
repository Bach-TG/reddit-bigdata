"""
Data Import Script for Reddit Visualization Dashboard
Loads data from CSV/JSONL files, performs NLP processing, and inserts into PostgreSQL

Features:
- Load from reddit_posts.csv or reddit_posts.jsonl
- Basic sentiment analysis (keyword-based)
- Topic classification
- Entity extraction (countries)
- Keyword extraction from titles
"""

import os
import sys
import json
import csv
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
import psycopg2
from psycopg2.extras import execute_values
import logging

# Increase CSV field size limit for large comments fields
csv.field_size_limit(10 * 1024 * 1024)  # 10 MB limit

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'reddit_db'),
    'user': os.getenv('POSTGRES_USER', 'reddit_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'reddit_pass')
}

# Data file paths
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
CSV_FILE = os.path.join(DATA_DIR, 'reddit_posts.csv')
JSONL_FILE = os.path.join(DATA_DIR, 'reddit_posts.jsonl')


# ============================================================================
# NLP PROCESSING FUNCTIONS
# ============================================================================

# Sentiment keywords
POSITIVE_WORDS = {
    'good', 'great', 'support', 'peace', 'success', 'win', 'victory', 
    'agree', 'love', 'happy', 'excellent', 'amazing', 'wonderful',
    'progress', 'improvement', 'positive', 'hope', 'freedom', 'democracy'
}

NEGATIVE_WORDS = {
    'war', 'conflict', 'death', 'attack', 'ban', 'threat', 'kill', 'dead',
    'crisis', 'disaster', 'fail', 'bad', 'terrible', 'horrible', 'corrupt',
    'violence', 'terrorist', 'genocide', 'invasion', 'sanctions', 'arrest'
}

# Countries for entity extraction
COUNTRIES = {
    'usa': 'USA', 'us': 'USA', 'united states': 'USA', 'america': 'USA',
    'china': 'China', 'chinese': 'China',
    'russia': 'Russia', 'russian': 'Russia', 'putin': 'Russia',
    'ukraine': 'Ukraine', 'ukrainian': 'Ukraine', 'zelensky': 'Ukraine',
    'israel': 'Israel', 'israeli': 'Israel', 'netanyahu': 'Israel',
    'iran': 'Iran', 'iranian': 'Iran',
    'uk': 'UK', 'britain': 'UK', 'british': 'UK', 'england': 'UK',
    'india': 'India', 'indian': 'India', 'modi': 'India',
    'pakistan': 'Pakistan',
    'germany': 'Germany', 'german': 'Germany',
    'france': 'France', 'french': 'France', 'macron': 'France',
    'japan': 'Japan', 'japanese': 'Japan',
    'north korea': 'North Korea', 'kim jong': 'North Korea',
    'south korea': 'South Korea', 'korea': 'South Korea',
    'taiwan': 'Taiwan',
    'gaza': 'Gaza', 'palestine': 'Palestine', 'palestinian': 'Palestine',
    'syria': 'Syria', 'syrian': 'Syria',
    'iraq': 'Iraq',
    'afghanistan': 'Afghanistan',
    'canada': 'Canada', 'canadian': 'Canada',
    'mexico': 'Mexico', 'mexican': 'Mexico',
    'brazil': 'Brazil',
    'australia': 'Australia',
    'trump': 'USA', 'biden': 'USA', 'musk': 'USA'
}

# Topic classification keywords
TOPIC_KEYWORDS = {
    'war_conflict': ['war', 'military', 'troops', 'defense', 'attack', 'invasion', 
                     'bomb', 'missile', 'army', 'soldier', 'battle', 'combat'],
    'politics': ['trump', 'president', 'election', 'minister', 'government', 
                 'congress', 'senate', 'vote', 'party', 'democrat', 'republican'],
    'trade_economy': ['trade', 'economy', 'deal', 'tariff', 'export', 'import',
                      'market', 'stock', 'gdp', 'inflation', 'debt', 'sanction'],
    'policy': ['ban', 'restrict', 'law', 'policy', 'rule', 'regulation', 
               'court', 'legal', 'justice', 'rights', 'reform']
}


def clean_text(text: str) -> str:
    """Clean text for NLP processing"""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'http\S+|www.\S+', '', text)  # Remove URLs
    text = re.sub(r'[^\w\s\.\,\!\?\-]', '', text)  # Remove special chars
    text = ' '.join(text.split())  # Normalize whitespace
    return text.strip()


def analyze_sentiment(text: str) -> tuple:
    """
    Simple keyword-based sentiment analysis
    Returns: (sentiment_score, sentiment_label)
    """
    if not text:
        return 0.0, 'neutral'
    
    text_lower = text.lower()
    words = set(text_lower.split())
    
    pos_count = len(words & POSITIVE_WORDS)
    neg_count = len(words & NEGATIVE_WORDS)
    
    total = pos_count + neg_count
    if total == 0:
        return 0.0, 'neutral'
    
    score = (pos_count - neg_count) / total
    
    if score > 0.1:
        label = 'positive'
    elif score < -0.1:
        label = 'negative'
    else:
        label = 'neutral'
    
    return round(score, 3), label


def classify_topic(text: str) -> str:
    """Classify text into topic category"""
    if not text:
        return 'other'
    
    text_lower = text.lower()
    
    topic_scores = {}
    for topic, keywords in TOPIC_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        topic_scores[topic] = score
    
    max_topic = max(topic_scores, key=topic_scores.get)
    if topic_scores[max_topic] > 0:
        return max_topic
    return 'other'


def extract_entities(text: str) -> List[str]:
    """Extract country/entity mentions from text"""
    if not text:
        return []
    
    text_lower = text.lower()
    found_entities = set()
    
    for keyword, entity in COUNTRIES.items():
        if keyword in text_lower:
            found_entities.add(entity)
    
    return list(found_entities)


def extract_keywords(text: str, top_n: int = 5) -> List[str]:
    """Extract top keywords from text (simple word frequency)"""
    if not text:
        return []
    
    # Common stop words to filter
    stop_words = {
        'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
        'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'dare',
        'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from', 'as',
        'into', 'through', 'during', 'before', 'after', 'above', 'below',
        'and', 'but', 'or', 'nor', 'so', 'yet', 'both', 'either', 'neither',
        'not', 'only', 'own', 'same', 'than', 'too', 'very', 'just',
        'that', 'this', 'these', 'those', 'what', 'which', 'who', 'whom',
        'how', 'when', 'where', 'why', 'all', 'each', 'every', 'some',
        'any', 'no', 'most', 'more', 'other', 'such', 'many', 'much',
        'it', 'its', 'he', 'she', 'they', 'we', 'you', 'i', 'me', 'my',
        'his', 'her', 'their', 'our', 'your', 'him', 'them', 'us'
    }
    
    # Clean and tokenize
    text_clean = clean_text(text)
    words = [w for w in text_clean.split() if len(w) > 2 and w not in stop_words]
    
    # Count frequency
    word_freq = {}
    for word in words:
        word_freq[word] = word_freq.get(word, 0) + 1
    
    # Get top keywords
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return [word for word, _ in sorted_words[:top_n]]


def get_engagement_category(score: int) -> str:
    """Categorize post engagement based on score"""
    if score < 10:
        return 'low'
    elif score < 50:
        return 'medium'
    elif score < 100:
        return 'high'
    else:
        return 'viral'


def parse_comments(comments_data) -> tuple:
    """
    Parse comments field from CSV/JSONL
    Returns: (comment_count, comments_text)
    """
    if not comments_data:
        return 0, ""
    
    try:
        if isinstance(comments_data, str):
            comments = json.loads(comments_data)
        else:
            comments = comments_data
        
        if isinstance(comments, list):
            comment_count = len(comments)
            comments_text = ' '.join(str(c) for c in comments[:10])  # First 10 comments
            return comment_count, comments_text[:5000]  # Limit text length
    except (json.JSONDecodeError, TypeError):
        pass
    
    return 0, ""


# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def load_from_csv(filepath: str) -> List[Dict[str, Any]]:
    """Load data from CSV file"""
    logger.info(f"📂 Loading from CSV: {filepath}")
    
    rows = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    
    logger.info(f"   Loaded {len(rows)} rows from CSV")
    return rows


def load_from_jsonl(filepath: str) -> List[Dict[str, Any]]:
    """Load data from JSONL file"""
    logger.info(f"📂 Loading from JSONL: {filepath}")
    
    rows = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    
    logger.info(f"   Loaded {len(rows)} rows from JSONL")
    return rows


def process_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single row with NLP analysis"""
    
    # Extract basic fields
    post_id = row.get('post_id', '')
    subreddit = row.get('subreddit', '')
    title = row.get('title', '')
    body = row.get('body', '') or ''
    author = row.get('author', '')
    created_utc = int(row.get('created_utc', 0))
    score = int(row.get('score', 0) or 0)
    
    # Parse comments
    comment_count, comments_text = parse_comments(row.get('comments'))
    
    # Convert timestamp
    created_datetime = datetime.fromtimestamp(created_utc) if created_utc > 0 else None
    
    # Clean text
    title_cleaned = clean_text(title)
    body_cleaned = clean_text(body)
    full_text = f"{title} {body}"
    
    # NLP Analysis
    sentiment_score, sentiment = analyze_sentiment(full_text)
    topic = classify_topic(full_text)
    entities = extract_entities(full_text)
    keywords = extract_keywords(full_text)
    
    # Text features
    title_word_count = len(title_cleaned.split())
    body_word_count = len(body_cleaned.split()) if body_cleaned else 0
    has_body = bool(body_cleaned)
    
    # Engagement
    engagement_category = get_engagement_category(score)
    
    # Time features
    hour_of_day = created_datetime.hour if created_datetime else None
    day_of_week = created_datetime.weekday() if created_datetime else None
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_name = day_names[day_of_week] if day_of_week is not None else None
    is_weekend = day_of_week in [5, 6] if day_of_week is not None else False
    
    return {
        'post_id': post_id,
        'subreddit': subreddit,
        'author': author,
        'created_utc': created_utc,
        'created_datetime': created_datetime,
        'score': score,
        'title': title,
        'body': body,
        'title_cleaned': title_cleaned,
        'body_cleaned': body_cleaned,
        'comment_count': comment_count,
        'comments_text': comments_text,
        'sentiment_score': sentiment_score,
        'sentiment': sentiment,
        'topic': topic,
        'entities': json.dumps(entities),
        'entity_count': len(entities),
        'keywords': json.dumps(keywords),
        'title_word_count': title_word_count,
        'body_word_count': body_word_count,
        'has_body': has_body,
        'engagement_category': engagement_category,
        'hour_of_day': hour_of_day,
        'day_of_week': day_of_week,
        'day_name': day_name,
        'is_weekend': is_weekend,
        'processing_timestamp': datetime.now()
    }


# ============================================================================
# DATABASE INSERT FUNCTIONS
# ============================================================================

def get_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)


def insert_posts(conn, processed_rows: List[Dict[str, Any]]):
    """Insert processed posts into database"""
    logger.info(f"📥 Inserting {len(processed_rows)} posts into database...")
    
    cursor = conn.cursor()
    
    # Prepare columns
    columns = [
        'post_id', 'subreddit', 'author', 'created_utc', 'created_datetime', 'score',
        'title', 'body', 'title_cleaned', 'body_cleaned', 'comment_count', 'comments_text',
        'sentiment_score', 'sentiment', 'topic', 'entities', 'entity_count', 'keywords',
        'title_word_count', 'body_word_count', 'has_body', 'engagement_category',
        'hour_of_day', 'day_of_week', 'day_name', 'is_weekend', 'processing_timestamp'
    ]
    
    # Prepare values
    values = [
        tuple(row[col] for col in columns)
        for row in processed_rows
    ]
    
    # Insert with ON CONFLICT
    insert_sql = f"""
        INSERT INTO reddit_posts ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (post_id) DO UPDATE SET
            score = EXCLUDED.score,
            processing_timestamp = EXCLUDED.processing_timestamp
    """
    
    execute_values(cursor, insert_sql, values, page_size=100)
    conn.commit()
    
    logger.info(f"   ✅ Inserted {len(processed_rows)} posts")
    cursor.close()


def populate_aggregated_tables(conn):
    """Populate aggregated statistics tables"""
    logger.info("📊 Populating aggregated tables...")
    
    cursor = conn.cursor()
    
    # 1. Populate subreddit_stats
    cursor.execute("""
        INSERT INTO subreddit_stats (subreddit, date, total_posts, total_score, 
                                      avg_score, max_score, avg_sentiment, 
                                      avg_title_length, posts_with_body, unique_authors)
        SELECT 
            subreddit,
            DATE(created_datetime) as date,
            COUNT(*) as total_posts,
            SUM(score) as total_score,
            AVG(score) as avg_score,
            MAX(score) as max_score,
            AVG(sentiment_score) as avg_sentiment,
            AVG(title_word_count) as avg_title_length,
            SUM(CASE WHEN has_body THEN 1 ELSE 0 END) as posts_with_body,
            COUNT(DISTINCT author) as unique_authors
        FROM reddit_posts
        WHERE created_datetime IS NOT NULL
        GROUP BY subreddit, DATE(created_datetime)
        ON CONFLICT (subreddit, date) DO UPDATE SET
            total_posts = EXCLUDED.total_posts,
            total_score = EXCLUDED.total_score,
            avg_score = EXCLUDED.avg_score,
            max_score = EXCLUDED.max_score,
            avg_sentiment = EXCLUDED.avg_sentiment
    """)
    logger.info("   ✅ Updated subreddit_stats")
    
    # 2. Populate sentiment_stats
    cursor.execute("""
        INSERT INTO sentiment_stats (date, subreddit, sentiment, count, avg_score)
        SELECT 
            DATE(created_datetime) as date,
            subreddit,
            sentiment,
            COUNT(*) as count,
            AVG(score) as avg_score
        FROM reddit_posts
        WHERE created_datetime IS NOT NULL AND sentiment IS NOT NULL
        GROUP BY DATE(created_datetime), subreddit, sentiment
        ON CONFLICT (date, subreddit, sentiment) DO UPDATE SET
            count = EXCLUDED.count,
            avg_score = EXCLUDED.avg_score
    """)
    logger.info("   ✅ Updated sentiment_stats")
    
    # 3. Populate trending_topics
    cursor.execute("""
        INSERT INTO trending_topics (date, topic, subreddit, mention_count, 
                                     avg_score, total_score, avg_sentiment, is_trending)
        SELECT 
            DATE(created_datetime) as date,
            topic,
            subreddit,
            COUNT(*) as mention_count,
            AVG(score) as avg_score,
            SUM(score) as total_score,
            AVG(sentiment_score) as avg_sentiment,
            COUNT(*) > 5 as is_trending
        FROM reddit_posts
        WHERE created_datetime IS NOT NULL AND topic IS NOT NULL
        GROUP BY DATE(created_datetime), topic, subreddit
        ON CONFLICT (date, topic, subreddit) DO UPDATE SET
            mention_count = EXCLUDED.mention_count,
            avg_score = EXCLUDED.avg_score,
            total_score = EXCLUDED.total_score
    """)
    logger.info("   ✅ Updated trending_topics")
    
    # 4. Populate hourly_patterns
    cursor.execute("""
        INSERT INTO hourly_patterns (subreddit, hour_of_day, post_count, 
                                     avg_score, total_score, avg_sentiment)
        SELECT 
            subreddit,
            hour_of_day,
            COUNT(*) as post_count,
            AVG(score) as avg_score,
            SUM(score) as total_score,
            AVG(sentiment_score) as avg_sentiment
        FROM reddit_posts
        WHERE hour_of_day IS NOT NULL
        GROUP BY subreddit, hour_of_day
        ON CONFLICT (subreddit, hour_of_day) DO UPDATE SET
            post_count = EXCLUDED.post_count,
            avg_score = EXCLUDED.avg_score,
            total_score = EXCLUDED.total_score
    """)
    logger.info("   ✅ Updated hourly_patterns")
    
    conn.commit()
    cursor.close()


def populate_keywords_table(conn, processed_rows: List[Dict[str, Any]]):
    """Populate post_keywords and keywords_trend tables"""
    logger.info("🔑 Populating keywords tables...")
    
    cursor = conn.cursor()
    
    # Insert keywords for each post
    keyword_rows = []
    for row in processed_rows:
        post_id = row['post_id']
        keywords = json.loads(row['keywords']) if row['keywords'] else []
        for keyword in keywords:
            keyword_rows.append((post_id, keyword, 1))
    
    if keyword_rows:
        execute_values(
            cursor,
            "INSERT INTO post_keywords (post_id, keyword, frequency) VALUES %s ON CONFLICT DO NOTHING",
            keyword_rows,
            page_size=100
        )
    
    # Aggregate keywords by date
    cursor.execute("""
        INSERT INTO keywords_trend (keyword, date, count, avg_score, avg_sentiment, subreddit)
        SELECT 
            pk.keyword,
            DATE(rp.created_datetime) as date,
            COUNT(*) as count,
            AVG(rp.score) as avg_score,
            AVG(rp.sentiment_score) as avg_sentiment,
            rp.subreddit
        FROM post_keywords pk
        JOIN reddit_posts rp ON pk.post_id = rp.post_id
        WHERE rp.created_datetime IS NOT NULL
        GROUP BY pk.keyword, DATE(rp.created_datetime), rp.subreddit
        ON CONFLICT (keyword, date, subreddit) DO UPDATE SET
            count = EXCLUDED.count,
            avg_score = EXCLUDED.avg_score
    """)
    
    conn.commit()
    logger.info(f"   ✅ Inserted {len(keyword_rows)} keyword mappings")
    cursor.close()


def get_stats(conn) -> Dict[str, Any]:
    """Get database statistics"""
    cursor = conn.cursor()
    
    stats = {}
    
    cursor.execute("SELECT COUNT(*) FROM reddit_posts")
    stats['total_posts'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT subreddit) FROM reddit_posts")
    stats['subreddits'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT subreddit, COUNT(*) as cnt FROM reddit_posts GROUP BY subreddit ORDER BY cnt DESC LIMIT 5")
    stats['top_subreddits'] = cursor.fetchall()
    
    cursor.execute("SELECT sentiment, COUNT(*) FROM reddit_posts GROUP BY sentiment")
    stats['sentiment_dist'] = dict(cursor.fetchall())
    
    cursor.execute("SELECT topic, COUNT(*) FROM reddit_posts GROUP BY topic ORDER BY COUNT(*) DESC")
    stats['topic_dist'] = dict(cursor.fetchall())
    
    cursor.close()
    return stats


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Import Reddit data to PostgreSQL')
    parser.add_argument('--source', choices=['csv', 'jsonl', 'auto'], default='auto',
                        help='Data source format')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rows to import')
    parser.add_argument('--skip-aggregates', action='store_true',
                        help='Skip populating aggregated tables')
    args = parser.parse_args()
    
    print("=" * 70)
    print("📊 Reddit Data Import for Visualization Dashboard")
    print("=" * 70)
    
    # Determine data source
    if args.source == 'auto':
        if os.path.exists(CSV_FILE):
            source_file = CSV_FILE
            load_func = load_from_csv
        elif os.path.exists(JSONL_FILE):
            source_file = JSONL_FILE
            load_func = load_from_jsonl
        else:
            logger.error(f"❌ No data file found in {DATA_DIR}")
            sys.exit(1)
    elif args.source == 'csv':
        source_file = CSV_FILE
        load_func = load_from_csv
    else:
        source_file = JSONL_FILE
        load_func = load_from_jsonl
    
    print(f"📂 Source: {source_file}")
    print(f"🗄️  Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print("=" * 70)
    
    # Load data
    raw_rows = load_func(source_file)
    
    if args.limit:
        raw_rows = raw_rows[:args.limit]
        logger.info(f"   Limited to {len(raw_rows)} rows")
    
    # Process rows with NLP
    print("\n🧠 Processing rows with NLP analysis...")
    processed_rows = []
    for i, row in enumerate(raw_rows):
        processed = process_row(row)
        processed_rows.append(processed)
        if (i + 1) % 200 == 0:
            logger.info(f"   Processed {i + 1}/{len(raw_rows)} rows...")
    
    logger.info(f"   ✅ Processed {len(processed_rows)} rows")
    
    # Connect to database
    conn = get_connection()
    
    try:
        # Insert posts
        insert_posts(conn, processed_rows)
        
        # Populate keywords
        populate_keywords_table(conn, processed_rows)
        
        # Populate aggregated tables
        if not args.skip_aggregates:
            populate_aggregated_tables(conn)
        
        # Get and display stats
        stats = get_stats(conn)
        
        print("\n" + "=" * 70)
        print("📈 Import Statistics")
        print("=" * 70)
        print(f"Total Posts: {stats['total_posts']}")
        print(f"Subreddits: {stats['subreddits']}")
        print(f"\nTop Subreddits:")
        for sub, cnt in stats['top_subreddits']:
            print(f"   • {sub}: {cnt} posts")
        print(f"\nSentiment Distribution:")
        for sentiment, cnt in stats['sentiment_dist'].items():
            print(f"   • {sentiment}: {cnt}")
        print(f"\nTopic Distribution:")
        for topic, cnt in stats['topic_dist'].items():
            print(f"   • {topic}: {cnt}")
        
        print("\n" + "=" * 70)
        print("✨ Import complete! Data ready for visualization.")
        print("=" * 70)
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
