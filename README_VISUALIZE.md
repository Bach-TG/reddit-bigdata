# Reddit Analytics Visualization Dashboard

## 📊 Overview

Dashboard trực quan hóa dữ liệu Reddit với các tính năng:

- **Subreddit Distribution**: Phân bố bài viết theo subreddit
- **Sentiment Analysis**: Phân tích cảm xúc (positive/negative/neutral)
- **Topic Classification**: Phân loại chủ đề (war_conflict, politics, trade_economy, policy)
- **Entity Extraction**: Trích xuất entities (các quốc gia được đề cập)
- **Time Patterns**: Phân tích mẫu thời gian (hourly, daily)
- **Engagement Metrics**: Chỉ số tương tác (score, comments)

## 🚀 Quick Start

### Option 1: Docker Compose (Recommended)

```bash
# 1. Start PostgreSQL and Dashboard
docker-compose -f docker-compose.visualize.yml up -d postgres dashboard

# 2. Import data (first time only)
docker-compose -f docker-compose.visualize.yml --profile import up data-import

# 3. Access dashboard
# Open http://localhost:8501
```

### Option 2: Local Development

```bash
# 1. Install dependencies
pip install -r visualize/requirements.txt

# 2. Start PostgreSQL (local or Docker)
docker-compose -f docker-compose.visualize.yml up -d postgres

# 3. Create tables
python scripts/create_tables.py

# 4. Import data
python scripts/import_data.py

# 5. Run dashboard
cd visualize
streamlit run dashboard.py
```

## 📁 Project Structure

```
reddit-bigdata/
├── data/
│   ├── reddit_posts.csv      # Raw data (~1454 posts)
│   └── reddit_posts.jsonl    # Raw data (JSONL format)
├── scripts/
│   ├── create_tables.py      # PostgreSQL schema
│   └── import_data.py        # Data import with NLP
├── visualize/
│   ├── dashboard.py          # Streamlit app
│   ├── requirements.txt      # Python dependencies
│   └── Dockerfile            # Container config
├── docker-compose.visualize.yml
└── README_VISUALIZE.md       # This file
```

## 🗄️ Database Schema

### Main Tables

| Table             | Description                                         |
| ----------------- | --------------------------------------------------- |
| `reddit_posts`    | Posts với NLP analysis (sentiment, topic, entities) |
| `post_keywords`   | Keywords trích xuất từ posts                        |
| `keywords_trend`  | Trend keywords theo thời gian                       |
| `sentiment_stats` | Thống kê sentiment aggregated                       |
| `subreddit_stats` | Thống kê theo subreddit                             |
| `trending_topics` | Chủ đề trending                                     |
| `hourly_patterns` | Mẫu posting theo giờ                                |

## 🔧 Configuration

### Environment Variables

| Variable            | Default     | Description     |
| ------------------- | ----------- | --------------- |
| `POSTGRES_HOST`     | localhost   | PostgreSQL host |
| `POSTGRES_PORT`     | 5432        | PostgreSQL port |
| `POSTGRES_DB`       | reddit_db   | Database name   |
| `POSTGRES_USER`     | reddit_user | Username        |
| `POSTGRES_PASSWORD` | reddit_pass | Password        |

## 📈 Dashboard Features

### Filters

- **Date Range**: Lọc theo khoảng thời gian
- **Subreddits**: Lọc theo subreddit
- **Keyword Search**: Tìm kiếm trong title/body

### Visualizations

1. **Overview Metrics**: Total posts, avg score, comments, subreddits, authors
2. **Subreddit Distribution**: Bar chart + Pie chart
3. **Sentiment Analysis**: Distribution pie, by subreddit, over time
4. **Topic Analysis**: Topic distribution, engagement by topic
5. **Entity Analysis**: Countries mentioned (bar + treemap)
6. **Time Patterns**: Hourly/daily patterns, heatmap
7. **Engagement Analysis**: Category distribution, score histogram
8. **Data Table**: Interactive searchable table

## 🔗 Integration with Spark Pipeline

Dashboard tương thích với output từ `phongtd11/spark_scripts/`:

- `spark_03_analytics_layer.py` writes to same PostgreSQL tables
- Can visualize real-time data from Spark Streaming

## 📝 NLP Processing

### Sentiment Analysis

- Keyword-based (positive/negative words)
- Score: -1.0 to 1.0
- Labels: positive (>0.1), negative (<-0.1), neutral

### Topic Classification

- war_conflict: war, military, troops, defense...
- politics: trump, president, election...
- trade_economy: trade, economy, tariff...
- policy: ban, restrict, law...
- other: everything else

### Entity Extraction

- Countries: USA, China, Russia, Ukraine, Israel, UK, Iran...
- Leaders: Trump, Biden, Putin, Zelensky, Macron...

## 🐛 Troubleshooting

### Dashboard shows "No data"

```bash
# Check if data was imported
docker exec -it reddit_postgres psql -U reddit_user -d reddit_db -c "SELECT COUNT(*) FROM reddit_posts"

# Re-import if needed
python scripts/import_data.py
```

### Connection refused

```bash
# Check PostgreSQL is running
docker ps | grep postgres

# Check port is available
netstat -an | grep 5432
```

## 📄 License

MIT License
