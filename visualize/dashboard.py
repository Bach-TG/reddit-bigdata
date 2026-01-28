"""
Reddit Analytics Dashboard - Streamlit Visualization
Connects to PostgreSQL database and displays interactive charts

Features:
- Overview metrics (posts, scores, subreddits)
- Subreddit distribution and comparison
- Sentiment analysis (over time, by subreddit, distribution)
- Topic/Entity analysis
- Time patterns (hourly, daily)
- Interactive filters (date, subreddit, keyword)
- Data table with search
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import json

# Page config
st.set_page_config(
    page_title="Reddit Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF4500;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #1f2937;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid #FF4500;
        padding-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_connection():
    """Create database connection"""
    try:
        dbname = os.getenv('POSTGRES_DB', 'reddit_db')
        user = os.getenv('POSTGRES_USER', 'reddit_user')
        password = os.getenv('POSTGRES_PASSWORD', 'reddit_pass')
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')

        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None


# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

@st.cache_data(ttl=300)
def load_posts_data(_engine, start_date, end_date, subreddits, keyword):
    """Load posts data with filters"""
    query = """
        SELECT post_id, subreddit, title, body, author, score, 
               created_datetime, sentiment, sentiment_score, topic,
               entities, entity_count, keywords, comment_count,
               title_word_count, engagement_category, hour_of_day, day_name
        FROM reddit_posts
        WHERE created_datetime BETWEEN :start_date AND :end_date
    """
    params = {"start_date": start_date, "end_date": end_date}

    if subreddits:
        query += " AND subreddit IN :subreddits"
        params["subreddits"] = tuple(subreddits)

    if keyword:
        query += " AND (LOWER(title) LIKE :keyword OR LOWER(body) LIKE :keyword)"
        params["keyword"] = f"%{keyword.lower()}%"

    query += " ORDER BY created_datetime DESC"

    return pd.read_sql(text(query), _engine, params=params)


@st.cache_data(ttl=300)
def load_subreddit_stats(_engine, start_date, end_date):
    """Load subreddit statistics"""
    query = """
        SELECT subreddit, date, total_posts, total_score, avg_score, 
               max_score, avg_sentiment, avg_title_length, posts_with_body, unique_authors
        FROM subreddit_stats
        WHERE date BETWEEN :start_date AND :end_date
        ORDER BY date DESC
    """
    return pd.read_sql(text(query), _engine, params={"start_date": start_date, "end_date": end_date})


@st.cache_data(ttl=300)
def load_sentiment_stats(_engine, start_date, end_date):
    """Load sentiment statistics"""
    query = """
        SELECT date, subreddit, sentiment, count, avg_score
        FROM sentiment_stats
        WHERE date BETWEEN :start_date AND :end_date
        ORDER BY date DESC
    """
    return pd.read_sql(text(query), _engine, params={"start_date": start_date, "end_date": end_date})


@st.cache_data(ttl=300)
def load_trending_topics(_engine, start_date, end_date):
    """Load trending topics"""
    query = """
        SELECT date, topic, subreddit, mention_count, avg_score, total_score, 
               avg_sentiment, is_trending
        FROM trending_topics
        WHERE date BETWEEN :start_date AND :end_date
        ORDER BY mention_count DESC
    """
    return pd.read_sql(text(query), _engine, params={"start_date": start_date, "end_date": end_date})


@st.cache_data(ttl=300)
def load_hourly_patterns(_engine):
    """Load hourly posting patterns"""
    query = """
        SELECT subreddit, hour_of_day, post_count, avg_score, avg_sentiment, is_peak_hour
        FROM hourly_patterns
        ORDER BY subreddit, hour_of_day
    """
    return pd.read_sql(text(query), _engine)


@st.cache_data(ttl=300)
def get_available_subreddits(_engine):
    """Get list of available subreddits"""
    query = "SELECT DISTINCT subreddit FROM reddit_posts ORDER BY subreddit"
    result = pd.read_sql(text(query), _engine)
    return result['subreddit'].tolist()


@st.cache_data(ttl=300)
def get_date_range(_engine):
    """Get min and max dates in data"""
    query = """
        SELECT MIN(created_datetime) as min_date, MAX(created_datetime) as max_date
        FROM reddit_posts
    """
    result = pd.read_sql(text(query), _engine)
    return result.iloc[0]['min_date'], result.iloc[0]['max_date']


# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def create_overview_metrics(df):
    """Create overview metrics cards"""
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("📝 Total Posts", f"{len(df):,}")
    with col2:
        st.metric("⬆️ Avg Score", f"{df['score'].mean():.1f}")
    with col3:
        st.metric("💬 Total Comments", f"{df['comment_count'].sum():,}")
    with col4:
        st.metric("📂 Subreddits", f"{df['subreddit'].nunique()}")
    with col5:
        st.metric("👤 Authors", f"{df['author'].nunique():,}")


def create_subreddit_distribution(df):
    """Create subreddit distribution charts"""
    st.markdown('<p class="section-header">📂 Subreddit Distribution</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Posts by subreddit
        subreddit_counts = df['subreddit'].value_counts().head(10)
        fig = px.bar(
            x=subreddit_counts.values,
            y=subreddit_counts.index,
            orientation='h',
            title="Posts by Subreddit (Top 10)",
            labels={'x': 'Number of Posts', 'y': 'Subreddit'},
            color=subreddit_counts.values,
            color_continuous_scale='Oranges'
        )
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Pie chart
        fig = px.pie(
            values=subreddit_counts.values,
            names=subreddit_counts.index,
            title="Subreddit Share",
            hole=0.4
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


def create_sentiment_analysis(df, sentiment_df):
    """Create sentiment analysis visualizations"""
    st.markdown('<p class="section-header">😊 Sentiment Analysis</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Sentiment distribution pie
        if 'sentiment' in df.columns:
            sentiment_counts = df['sentiment'].value_counts()
            colors = {'positive': '#00CC96', 'neutral': '#636EFA', 'negative': '#EF553B'}
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title="Sentiment Distribution",
                color=sentiment_counts.index,
                color_discrete_map=colors
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Sentiment by subreddit
        if 'sentiment' in df.columns and 'subreddit' in df.columns:
            sentiment_by_sub = df.groupby(['subreddit', 'sentiment']).size().unstack(fill_value=0)
            if not sentiment_by_sub.empty:
                fig = px.bar(
                    sentiment_by_sub,
                    title="Sentiment by Subreddit",
                    barmode='stack',
                    color_discrete_map={'positive': '#00CC96', 'neutral': '#636EFA', 'negative': '#EF553B'}
                )
                fig.update_layout(height=350, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
    
    # Sentiment over time
    if not sentiment_df.empty and 'date' in sentiment_df.columns:
        st.subheader("Sentiment Over Time")
        sentiment_time = sentiment_df.groupby(['date', 'sentiment'])['count'].sum().unstack(fill_value=0)
        if not sentiment_time.empty:
            fig = px.area(
                sentiment_time,
                title="Sentiment Trend",
                color_discrete_map={'positive': '#00CC96', 'neutral': '#636EFA', 'negative': '#EF553B'}
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)


def create_topic_analysis(df, topics_df):
    """Create topic analysis visualizations"""
    st.markdown('<p class="section-header">🏷️ Topic Analysis</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Topic distribution
        if 'topic' in df.columns:
            topic_counts = df['topic'].value_counts()
            fig = px.bar(
                x=topic_counts.index,
                y=topic_counts.values,
                title="Topic Distribution",
                labels={'x': 'Topic', 'y': 'Count'},
                color=topic_counts.values,
                color_continuous_scale='Viridis'
            )
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Topic by engagement
        if 'topic' in df.columns and 'score' in df.columns:
            topic_engagement = df.groupby('topic').agg({
                'score': 'mean',
                'post_id': 'count'
            }).reset_index()
            topic_engagement.columns = ['topic', 'avg_score', 'count']
            
            fig = px.scatter(
                topic_engagement,
                x='count',
                y='avg_score',
                text='topic',
                title="Topic: Count vs Engagement",
                labels={'count': 'Number of Posts', 'avg_score': 'Average Score'},
                size='count'
            )
            fig.update_traces(textposition='top center')
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)


def create_entity_analysis(df):
    """Create entity (countries) analysis"""
    st.markdown('<p class="section-header">🌍 Entity Analysis (Countries Mentioned)</p>', unsafe_allow_html=True)
    
    # Extract entities from JSON
    all_entities = []
    for entities_json in df['entities'].dropna():
        try:
            entities = json.loads(entities_json) if isinstance(entities_json, str) else entities_json
            all_entities.extend(entities)
        except:
            pass
    
    if all_entities:
        entity_counts = pd.Series(all_entities).value_counts().head(15)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                x=entity_counts.values,
                y=entity_counts.index,
                orientation='h',
                title="Most Mentioned Entities",
                labels={'x': 'Mentions', 'y': 'Entity'},
                color=entity_counts.values,
                color_continuous_scale='Blues'
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Word cloud style visualization
            fig = px.treemap(
                names=entity_counts.index,
                parents=[''] * len(entity_counts),
                values=entity_counts.values,
                title="Entity Treemap"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No entity data available")


def create_time_patterns(df, hourly_df):
    """Create time pattern visualizations"""
    st.markdown('<p class="section-header">⏰ Time Patterns</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Posts by hour
        if 'hour_of_day' in df.columns:
            hourly_counts = df['hour_of_day'].value_counts().sort_index()
            fig = px.bar(
                x=hourly_counts.index,
                y=hourly_counts.values,
                title="Posts by Hour of Day (UTC)",
                labels={'x': 'Hour', 'y': 'Number of Posts'},
                color=hourly_counts.values,
                color_continuous_scale='Sunset'
            )
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Posts by day of week
        if 'day_name' in df.columns:
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            day_counts = df['day_name'].value_counts().reindex(day_order, fill_value=0)
            fig = px.bar(
                x=day_counts.index,
                y=day_counts.values,
                title="Posts by Day of Week",
                labels={'x': 'Day', 'y': 'Number of Posts'},
                color=day_counts.values,
                color_continuous_scale='Purples'
            )
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    # Heatmap: Hour vs Day
    if 'hour_of_day' in df.columns and 'day_name' in df.columns:
        st.subheader("Activity Heatmap")
        heatmap_data = df.groupby(['day_name', 'hour_of_day']).size().unstack(fill_value=0)
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        heatmap_data = heatmap_data.reindex(day_order)
        
        fig = px.imshow(
            heatmap_data,
            title="Posting Activity Heatmap (Day vs Hour)",
            labels=dict(x="Hour of Day", y="Day of Week", color="Posts"),
            aspect="auto",
            color_continuous_scale='YlOrRd'
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)


def create_engagement_analysis(df):
    """Create engagement analysis"""
    st.markdown('<p class="section-header">📈 Engagement Analysis</p>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Engagement category distribution
        if 'engagement_category' in df.columns:
            eng_counts = df['engagement_category'].value_counts()
            order = ['low', 'medium', 'high', 'viral']
            eng_counts = eng_counts.reindex([o for o in order if o in eng_counts.index])
            
            colors = {'low': '#636EFA', 'medium': '#00CC96', 'high': '#FFA15A', 'viral': '#EF553B'}
            fig = px.pie(
                values=eng_counts.values,
                names=eng_counts.index,
                title="Engagement Categories",
                color=eng_counts.index,
                color_discrete_map=colors
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Score distribution
        fig = px.histogram(
            df,
            x='score',
            nbins=50,
            title="Score Distribution",
            labels={'score': 'Score', 'count': 'Number of Posts'}
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)


def create_data_table(df):
    """Create interactive data table"""
    st.markdown('<p class="section-header">📋 Data Table</p>', unsafe_allow_html=True)
    
    # Select columns to display
    display_cols = ['created_datetime', 'subreddit', 'title', 'author', 'score', 
                    'sentiment', 'topic', 'comment_count']
    display_cols = [c for c in display_cols if c in df.columns]
    
    display_df = df[display_cols].copy()
    
    # Format datetime
    if 'created_datetime' in display_df.columns:
        display_df['created_datetime'] = pd.to_datetime(display_df['created_datetime']).dt.strftime('%Y-%m-%d %H:%M')
    
    # Truncate long titles
    if 'title' in display_df.columns:
        display_df['title'] = display_df['title'].str[:100] + '...'
    
    st.dataframe(
        display_df.head(100),
        use_container_width=True,
        height=400
    )
    
    st.caption(f"Showing {min(100, len(df))} of {len(df)} posts")


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main dashboard function"""
    # Header
    st.markdown('<h1 class="main-header">📊 Reddit Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Get database connection
    engine = get_connection()
    if engine is None:
        st.error("❌ Failed to connect to database. Please check PostgreSQL is running.")
        st.info("""
        **Setup Instructions:**
        1. Start PostgreSQL server
        2. Run: `python scripts/create_tables.py`
        3. Run: `python scripts/import_data.py`
        4. Restart this dashboard
        """)
        return
    
    # Check if data exists
    try:
        min_date, max_date = get_date_range(engine)
        if min_date is None:
            st.warning("⚠️ No data found in database. Please run import_data.py first.")
            return
    except Exception as e:
        st.error(f"Error reading data: {e}")
        return
    
    # ===== SIDEBAR FILTERS =====
    st.sidebar.header("🔍 Filters")
    
    # Date Range Filter
    st.sidebar.subheader("📅 Date Range")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "From",
            value=max(min_date.date() if hasattr(min_date, 'date') else min_date, 
                     (datetime.now() - timedelta(days=30)).date()),
            min_value=min_date.date() if hasattr(min_date, 'date') else min_date,
            max_value=max_date.date() if hasattr(max_date, 'date') else max_date,
            key="start_date"
        )
    with col2:
        end_date = st.date_input(
            "To",
            value=max_date.date() if hasattr(max_date, 'date') else max_date,
            min_value=min_date.date() if hasattr(min_date, 'date') else min_date,
            max_value=max_date.date() if hasattr(max_date, 'date') else max_date,
            key="end_date"
        )
    
    # Subreddit Filter
    st.sidebar.subheader("📂 Subreddits")
    available_subreddits = get_available_subreddits(engine)
    selected_subreddits = st.sidebar.multiselect(
        "Select Subreddits",
        options=available_subreddits,
        default=None,
        placeholder="All Subreddits"
    )
    
    # Keyword Filter
    st.sidebar.subheader("🔎 Keyword Search")
    keyword_filter = st.sidebar.text_input(
        "Search in titles/body",
        placeholder="e.g., Trump, Ukraine, China..."
    )
    
    # Refresh Button
    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    # ===== LOAD DATA =====
    with st.spinner("Loading data..."):
        df = load_posts_data(engine, start_date, end_date, selected_subreddits, keyword_filter)
        subreddit_df = load_subreddit_stats(engine, start_date, end_date)
        sentiment_df = load_sentiment_stats(engine, start_date, end_date)
        topics_df = load_trending_topics(engine, start_date, end_date)
        hourly_df = load_hourly_patterns(engine)
    
    if df.empty:
        st.warning("⚠️ No data found for the selected filters. Try adjusting the date range or subreddits.")
        return
    
    # ===== VISUALIZATIONS =====
    
    # Overview Metrics
    create_overview_metrics(df)
    
    st.divider()
    
    # Subreddit Distribution
    create_subreddit_distribution(df)
    
    st.divider()
    
    # Sentiment Analysis
    create_sentiment_analysis(df, sentiment_df)
    
    st.divider()
    
    # Topic Analysis
    create_topic_analysis(df, topics_df)
    
    st.divider()
    
    # Entity Analysis
    create_entity_analysis(df)
    
    st.divider()
    
    # Time Patterns
    create_time_patterns(df, hourly_df)
    
    st.divider()
    
    # Engagement Analysis
    create_engagement_analysis(df)
    
    st.divider()
    
    # Data Table
    create_data_table(df)
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"<center>📊 Reddit Analytics Dashboard | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
