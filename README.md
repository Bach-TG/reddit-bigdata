# Reddit Data Collection – Big Data Project

This project collects Reddit data from news & politics subreddits, streams the data to Kafka, and exports the results to JSONL and CSV files for further analysis and submission.

---

## Requirements

Make sure the following tools are installed:

- Docker (Docker Desktop)
- Python 3.10+
- uv (Python package manager)

Check versions:

```bash
docker --version
python --version
uv --version
```

---

## Step 1: Start Kafka (Docker Compose)

Kafka must be running before executing the Python ingestion script.

```bash
cd kafka
docker compose up -d
```

Verify containers are running:

```bash
docker ps
```

You should see containers such as:
- `zookeeper`
- `kafka1`

### (Optional) Create Kafka topic

```bash
docker exec -it kafka1 kafka-topics   --create   --topic reddit_data   --bootstrap-server kafka1:9092   --partitions 3   --replication-factor 1
```

---

## Step 2: Install Python dependencies (uv)

Return to the project root directory:

```bash
cd ..
```

Install dependencies:

```bash
uv sync
```

---

## Step 3: Run Reddit ingestion

Run the ingestion script:

```bash
uv run python data-collection/reddit.py
```

The script will:

- Periodically fetch the latest posts from multiple subreddits
- Stream data into Kafka topic `reddit_data`
- Export data to local files:
  - `data/reddit_posts.jsonl`
  - `data/reddit_posts.csv`

The script runs continuously and can be stopped at any time with:

```bash
Ctrl + C
```

---

## Output Files

Collected data is written to:

```
data/
├── reddit_posts.jsonl   # One JSON object per line
└── reddit_posts.csv     # CSV format
```

These files can be uploaded directly to Google Drive for submission or analysis.

---

## (Optional) View Kafka messages

To verify that data is being sent to Kafka:

```bash
docker exec -it kafka1 kafka-console-consumer   --bootstrap-server kafka1:9092   --topic reddit_data   --from-beginning
```

---

## Stop Kafka

When finished:

```bash
cd kafka
docker compose down
```

---

## Quick Start

```bash
cd kafka && docker compose up -d
cd .. && uv sync
uv run python data-collection/reddit.py
```
