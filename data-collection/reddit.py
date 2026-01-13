from typing import List, Dict, Any
import json
import datetime
import logging
import time
import praw
from prawcore.exceptions import PrawcoreException

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO, force=True)

# Kafka configuration
KAFKA_TOPIC = "reddit_data"
# KAFKA_BOOTSTRAP_SERVERS = ["kafka:9092"]
KAFKA_BOOTSTRAP_SERVERS = ["kafka1:9092"]
KAFKA_LOCAL_BOOTSTRAP_SERVERS = ["localhost:9094"]

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
FETCH_LIMIT = 50 

def create_reddit_client() -> praw.Reddit:
    """
    Creates and returns a Reddit client instance
    """
    return praw.Reddit(
        client_id="Db5HxiXppDbz_2yVdQk8Yg",
        client_secret="bUN0iWx8_dparp05-7JQlledUx0wBA",
        user_agent="python:Streaming_Kafka_Project:1.0 (by /u/New-Deer-1312)"
    )

def process_submission(submission: praw.models.Submission) -> Dict[str, Any]:
    """
    Processes a Reddit submission and returns a dictionary with relevant information
    """

    body = submission.selftext if getattr(submission, "is_self", False) else ""
    author = submission.author.name if submission.author else None
    
    return {
        "post_id": submission.id,
        "subreddit": submission.subreddit.display_name,
        "title": submission.title or "",
        "body": body,
        "author": author,
        "created_utc": int(submission.created_utc),
        "score": int(submission.score or 0),
    }

def get_reddit_data(reddit: praw.Reddit) -> List[Dict[str, Any]]:
    """
    Fetches recent submissions from multiple subreddits
    """
    all_submissions = []
    
    for subreddit_name in SUBREDDITS:
        try:
            subreddit = reddit.subreddit(subreddit_name)
            submissions = subreddit.new(limit=FETCH_LIMIT)
            processed_submissions = [process_submission(submission) for submission in submissions]
            all_submissions.extend(processed_submissions)
            logging.info(f"Fetched {len(processed_submissions)} submissions from r/{subreddit_name}")
            logging.info(f"Details: {processed_submissions}")
        except PrawcoreException as e:
            logging.error(f"Error fetching data from r/{subreddit_name}: {e}")
    
    return all_submissions

def data_collection() -> None:
    """
    Experimental function to collect data from Reddit
    """
    try:
        reddit = create_reddit_client()
        
        submissions = get_reddit_data(reddit)
        
        if submissions:
            logging.info(f"Fetched {len(submissions)} submissions from Reddit")
            
            for submission in submissions:
                logging.info(f"Processing submission: {submission['title']}")
                logging.info(f"Details: {submission}")
        
    except Exception as e:
        logging.error(f"Error in main loop: {e}")
        return False

if __name__ == "__main__":
    data_collection()
