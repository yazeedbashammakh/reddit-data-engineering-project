from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

class Config:
    CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
    CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
    USER_AGENT = os.getenv("REDDIT_USER_AGENT")
    BUCKET = os.getenv("GCS_BUCKET")
    GCS_FOLDER = os.getenv("GCS_FOLDER")
    STAGE_DATASET_ID = os.getenv("STAGE_DATASET_ID")
    FINAL_DATASET_ID = os.getenv("FINAL_DATASET_ID")
    GCP_PROJECT = os.getenv("GCP_PROJECT")

    TABLES = {
        "subreddit": "subreddits",
        "posts": "subreddit_posts",
        "comments": "subreddit_comments"
    }
