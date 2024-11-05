import sys
from datetime import datetime, timedelta, timezone
import pandas as pd
from praw import Reddit
from praw.models import subreddits
from utils.config import Config

from utils.logging import get_logger

logger = get_logger(__name__)

class RedditService:

    def __init__(self):
        try:
                
            self.reddit = Reddit(
                client_id=Config.CLIENT_ID,
                client_secret=Config.CLIENT_SECRET,
                user_agent=Config.USER_AGENT
            )
            logger.info("connected to reddit!")
        except Exception as e:
            logger.error(e)
            sys.exit(1)


    def get_subreddit(self, subreddit_name: str) -> subreddits:
        subreddit = self.reddit.subreddit(subreddit_name)
        return subreddit


    def fetch_raw_posts(self, subreddit, start_date=None, end_date=datetime.now(timezone.utc), limit=2000):
        """
        Fetch posts from a subreddit.
        Args:
        - subreddit: subreddit object
        - start_date: Start date in utc timezone
        - end_date: end date in utc timezone
        """
        if not start_date:
            start_date = end_date - timedelta(days=2)

        assert start_date <= end_date, f"start date cannot be greater than end date. Provided start date: {start_date}, end date: {end_date}"

        posts = []
        post_count = 0
        # Loop over each post
        for post in subreddit.new(limit=None):
            # If limit is reached than break
            if post_count >= limit:
                break
            post_date = datetime.fromtimestamp(post.created_utc, tz=timezone.utc)
            # bereak if post date is less than start date
            if post_date < start_date:
                break

            # check if post date is in between of desired limit
            if post_date <= end_date:
                post_count += 1
                posts.append(post)
        return posts


    def get_comments(self, post):
        """ get comments of a post"""
        post.comments.replace_more(limit=None)
        return post.comments.list()

