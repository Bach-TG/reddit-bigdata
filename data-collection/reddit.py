from __future__ import annotations

from typing import List, Dict, Any, Set, Optional
import os
import json
import csv
import time
import random
import logging
import datetime

from kafka import KafkaProducer
import kafka.errors

import praw
from prawcore.exceptions import (
    PrawcoreException,
    RequestException,
    ResponseException,
    ServerError,
    TooManyRequests,
    Forbidden,
    NotFound,
)

# =========================
# Logging
# =========================
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    force=True,
)

# =========================
# Kafka config
# =========================
KAFKA_TOPIC = "reddit_data"
KAFKA_BOOTSTRAP_SERVERS = ["kafka1:9092"]          # inside docker network
KAFKA_LOCAL_BOOTSTRAP_SERVERS = ["localhost:9094"] # host access (mapped port)

# =========================
# Reddit config (prefer env vars)
# =========================
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "Db5HxiXppDbz_2yVdQk8Yg")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "bUN0iWx8_dparp05-7JQlledUx0wBA")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT", "python:Streaming_Kafka_Project:1.0 (by /u/New-Deer-1312)")

SUBREDDITS = [
    "worldnews",
    "news",
    "politics",
    "PoliticalHumor",
    "Conservative",
    "geopolitics",
    "ukpolitics",
    "anime_titties",
    "moderatepolitics",
    "NeutralPolitics",
    "PoliticalDiscussion",
]

# =========================
# Ingestion behavior
# =========================
FETCH_LIMIT = 50              # max posts per subreddit per polling round
POLL_INTERVAL_SEC = 30        # polling interval for "realtime"
RUN_FOR_SEC: Optional[int] = None  # set e.g. 15*60 to auto-stop after 15 minutes
SEEN_CACHE_MAX = 50000        # dedup cache limit (avoid infinite growth)

# =========================
# Comments behavior (NEW)
# =========================
FETCH_ALL_COMMENTS = True     # True = expand all "MoreComments" and fetch all comments
COMMENTS_MAX_ATTEMPTS = 6     # retry attempts when fetching comments
COMMENTS_BASE_DELAY_SEC = 1.0 # base delay between comment API operations
COMMENTS_DELAY_CAP_SEC = 60.0 # cap delay

# =========================
# Export files
# =========================
OUT_DIR = "data"
OUT_JSONL = os.path.join(OUT_DIR, "reddit_posts.jsonl")  # append mode
OUT_CSV = os.path.join(OUT_DIR, "reddit_posts.csv")      # append mode
WRITE_HEADER_IF_EMPTY = True

# Schema fields (as required in your plan) + comments (NEW)
SCHEMA_FIELDS = ["post_id", "subreddit", "title", "body", "author", "created_utc", "score", "comments"]


# -------------------------
# Helpers
# -------------------------
def ensure_out_dir() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)


def sleep_with_jitter(base: float, cap: float = 60.0) -> None:
    time.sleep(min(cap, base) + random.uniform(0, 0.5))


def create_reddit_client() -> praw.Reddit:
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )
    reddit.read_only = True
    return reddit


def create_kafka_producer() -> KafkaProducer:
    common_kwargs = dict(
        acks="all",
        retries=3,
        linger_ms=50,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    )

    # 1) host-first (localhost)
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_LOCAL_BOOTSTRAP_SERVERS, **common_kwargs)
        logging.info(f"Kafka connected via localhost: {KAFKA_LOCAL_BOOTSTRAP_SERVERS}")
        return producer
    except kafka.errors.NoBrokersAvailable:
        logging.warning("No broker on localhost. Trying docker network...")

    # 2) docker-network fallback
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, **common_kwargs)
    logging.info(f"Kafka connected via docker network: {KAFKA_BOOTSTRAP_SERVERS}")
    return producer


# -------------------------
# Comments fetch (NEW)
# -------------------------
def fetch_all_comments_text(submission: praw.models.Submission) -> List[str]:
    """
    Fetch *all* comments for a submission.
    - Expands MoreComments (replace_more(limit=None))
    - Retries on transient errors + rate limit (429)
    - Returns list of comment bodies (strings)
    """
    attempt = 0
    comments: List[str] = []

    # Optional: choose ordering to reduce churn
    # 'top' often returns stable high-quality comments; 'new' returns most recent
    submission.comment_sort = "top"

    while True:
        attempt += 1
        try:
            if FETCH_ALL_COMMENTS:
                # This can be heavy but is the correct way to get "all comments"
                submission.comments.replace_more(limit=None)

            # Throttle a bit after expansion to avoid burst
            sleep_with_jitter(COMMENTS_BASE_DELAY_SEC, cap=COMMENTS_DELAY_CAP_SEC)

            # Flatten full comment tree
            for c in submission.comments.list():
                body = getattr(c, "body", None)
                if isinstance(body, str) and body.strip():
                    comments.append(body.strip())

            return comments

        except TooManyRequests as e:
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None:
                wait = float(retry_after) + random.uniform(0, 0.5)
                logging.warning(f"429 TooManyRequests while fetching comments, retry_after={retry_after}s")
                time.sleep(wait)
            else:
                backoff = 2 ** min(attempt, 5)
                logging.warning(f"429 TooManyRequests while fetching comments, backoff={backoff}s")
                sleep_with_jitter(backoff, cap=COMMENTS_DELAY_CAP_SEC)

        except (RequestException, ResponseException, ServerError) as e:
            if attempt >= COMMENTS_MAX_ATTEMPTS:
                logging.error(f"Transient error fetching comments after {attempt} attempts: {e}")
                return []
            backoff = 2 ** min(attempt, 5)
            logging.warning(f"Transient error fetching comments: {e} | retry in ~{backoff}s")
            sleep_with_jitter(backoff, cap=COMMENTS_DELAY_CAP_SEC)

        except PrawcoreException as e:
            if attempt >= COMMENTS_MAX_ATTEMPTS:
                logging.error(f"PRAW error fetching comments after {attempt} attempts: {e}")
                return []
            backoff = 2 ** min(attempt, 5)
            logging.warning(f"PRAW error fetching comments: {e} | retry in ~{backoff}s")
            sleep_with_jitter(backoff, cap=COMMENTS_DELAY_CAP_SEC)

        except Exception as e:
            # Non-PRAW unexpected issues
            logging.error(f"Unexpected error fetching comments: {e}")
            return []


def process_submission(submission: praw.models.Submission) -> Dict[str, Any]:
    """
    Required schema (+ comments):
      post_id, subreddit, title, body, author, created_utc, score, comments
    """
    body = submission.selftext if getattr(submission, "is_self", False) else ""
    author = submission.author.name if submission.author else None

    # NEW: fetch all comments (can be heavy)
    comments = fetch_all_comments_text(submission)

    return {
        "post_id": submission.id,
        "subreddit": submission.subreddit.display_name,
        "title": submission.title or "",
        "body": body or "",
        "author": author,
        "created_utc": int(submission.created_utc),
        "score": int(getattr(submission, "score", 0) or 0),
        "comments": comments,
    }


def fetch_subreddit_new_with_retry(
    reddit: praw.Reddit,
    subreddit_name: str,
    limit: int,
    max_attempts: int = 6,
) -> List[Dict[str, Any]]:
    """
    Fetch r/<sub>.new() with retry/backoff + handle 429.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            subreddit = reddit.subreddit(subreddit_name)
            submissions = subreddit.new(limit=limit)

            rows: List[Dict[str, Any]] = []
            for s in submissions:
                rows.append(process_submission(s))
            return rows

        except TooManyRequests as e:
            retry_after = getattr(e, "retry_after", None)
            if retry_after is not None:
                logging.warning(f"429 TooManyRequests r/{subreddit_name}, retry_after={retry_after}s")
                time.sleep(float(retry_after) + random.uniform(0, 0.5))
            else:
                backoff = 2 ** min(attempt, 5)
                logging.warning(f"429 TooManyRequests r/{subreddit_name}, backoff={backoff}s")
                sleep_with_jitter(backoff)

        except (RequestException, ResponseException, ServerError) as e:
            if attempt >= max_attempts:
                logging.error(f"Transient error r/{subreddit_name} after {attempt} attempts: {e}")
                return []
            backoff = 2 ** min(attempt, 5)
            logging.warning(f"Transient error r/{subreddit_name}: {e} | retry in ~{backoff}s")
            sleep_with_jitter(backoff)

        except (Forbidden, NotFound) as e:
            logging.error(f"Access issue r/{subreddit_name}: {e}")
            return []

        except PrawcoreException as e:
            if attempt >= max_attempts:
                logging.error(f"PRAW error r/{subreddit_name} after {attempt} attempts: {e}")
                return []
            backoff = 2 ** min(attempt, 5)
            logging.warning(f"PRAW error r/{subreddit_name}: {e} | retry in ~{backoff}s")
            sleep_with_jitter(backoff)


def get_reddit_data(reddit: praw.Reddit) -> List[Dict[str, Any]]:
    """
    One polling round: fetch from all subreddits.
    """
    all_rows: List[Dict[str, Any]] = []
    for subreddit_name in SUBREDDITS:
        rows = fetch_subreddit_new_with_retry(reddit, subreddit_name, FETCH_LIMIT)
        logging.info(f"Fetched {len(rows)} submissions from r/{subreddit_name}")
        all_rows.extend(rows)
        sleep_with_jitter(0.6, cap=3.0)  # gentle pacing
    return all_rows


# -------------------------
# Exporters (JSONL + CSV)
# -------------------------
def append_jsonl(path: str, row: Dict[str, Any]) -> None:
    ensure_out_dir()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def csv_needs_header(path: str) -> bool:
    if not WRITE_HEADER_IF_EMPTY:
        return False
    if not os.path.exists(path):
        return True
    return os.path.getsize(path) == 0


def append_csv(path: str, row: Dict[str, Any]) -> None:
    """
    CSV cannot store list directly -> store `comments` as JSON string.
    """
    ensure_out_dir()
    needs_header = csv_needs_header(path)

    row_for_csv = {k: row.get(k) for k in SCHEMA_FIELDS}
    row_for_csv["comments"] = json.dumps(row.get("comments", []), ensure_ascii=False)

    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SCHEMA_FIELDS)
        if needs_header:
            w.writeheader()
        w.writerow(row_for_csv)


# -------------------------
# Kafka callbacks
# -------------------------
def on_send_success(record_metadata) -> None:
    logging.debug(
        f"Sent to topic={record_metadata.topic} partition={record_metadata.partition} offset={record_metadata.offset}"
    )


def on_send_error(excp: BaseException) -> None:
    logging.error(f"Kafka send failed: {excp}")


# -------------------------
# Main loop: Reddit -> Kafka + JSONL + CSV
# -------------------------
def stream_to_kafka_and_export() -> None:
    reddit = create_reddit_client()
    producer = create_kafka_producer()

    seen: Set[str] = set()
    start_ts = time.time()

    logging.info(
        f"Start pipeline: topic={KAFKA_TOPIC} | poll={POLL_INTERVAL_SEC}s | "
        f"jsonl={OUT_JSONL} | csv={OUT_CSV}"
    )

    try:
        while True:
            if RUN_FOR_SEC is not None and (time.time() - start_ts) >= RUN_FOR_SEC:
                logging.info(f"Reached RUN_FOR_SEC={RUN_FOR_SEC}. Stopping...")
                break

            rows = get_reddit_data(reddit)

            sent_new = 0
            exported_new = 0

            for row in rows:
                pid = row["post_id"]
                if pid in seen:
                    continue

                # 1) Kafka
                fut = producer.send(KAFKA_TOPIC, key=pid, value=row)
                fut.add_callback(on_send_success).add_errback(on_send_error)

                # 2) Export files
                append_jsonl(OUT_JSONL, row)
                append_csv(OUT_CSV, row)

                seen.add(pid)
                sent_new += 1
                exported_new += 1

                # prevent seen set from growing forever
                if len(seen) > SEEN_CACHE_MAX:
                    for _ in range(SEEN_CACHE_MAX // 10):
                        seen.pop()

            producer.flush()
            logging.info(
                f"Polling done. fetched={len(rows)} sent_new={sent_new} exported_new={exported_new} seen_cache={len(seen)}"
            )

            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt. Closing producer...")
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass
        logging.info("Done.")


if __name__ == "__main__":
    stream_to_kafka_and_export()
