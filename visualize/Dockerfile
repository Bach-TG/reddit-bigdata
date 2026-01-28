# Streamlit Dashboard for Reddit Analytics
# Multi-stage build for smaller image size

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dashboard code
COPY dashboard.py .

# Create Streamlit config directory
RUN mkdir -p ~/.streamlit

# Streamlit configuration
RUN echo "\
[server]\n\
headless = true\n\
enableCORS = false\n\
port = 8501\n\
address = 0.0.0.0\n\
\n\
[theme]\n\
primaryColor = '#FF4500'\n\
backgroundColor = '#FFFFFF'\n\
secondaryBackgroundColor = '#F0F2F6'\n\
textColor = '#1F2937'\n\
font = 'sans serif'\n\
" > ~/.streamlit/config.toml

# Environment variables for PostgreSQL connection
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=reddit_db
ENV POSTGRES_USER=reddit_user
ENV POSTGRES_PASSWORD=reddit_pass

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Run Streamlit
ENTRYPOINT ["streamlit", "run", "dashboard.py"]
