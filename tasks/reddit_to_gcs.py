from datetime import datetime, timezone
import json

from utils.config import Config
from services.reddit import RedditService
from services.gcs import GCSService
from utils.logging import get_logger

log = get_logger(__name__)

def fetch_subreddit_to_gcs(subreddit_name: str):
    """
    This function crawl a subreddit.
    Output include subreddit details, its posts, comments and authors.
    Output is stored in a Json file in GCS
    
    Args:
        - subreddit_name: str
    """
    reddit_service = RedditService()

    subreddit = reddit_service.get_subreddit(subreddit_name)

    subreddit_details = parse_subreddit_details(subreddit)

    end_date = datetime.now(timezone.utc)
    end_date_str = end_date.strftime("%Y%m%dT%H:%M:%S")
    raw_posts = reddit_service.fetch_raw_posts(subreddit=subreddit, end_date=end_date)

    posts = parse_post_details(raw_posts)
    print(f"Total posts found: {len(posts)}")

    comments = parse_comment_details(reddit_service, raw_posts)
    print(f"Total comments found: {len(comments)}")

    
    gcs_path = f"{Config.GCS_FOLDER}/{subreddit_name}/{end_date_str}"
    subreddit_file_path = f"{gcs_path}/subreddit.json"
    posts_file_path = f"{gcs_path}/posts.json"
    comments_file_path = f"{gcs_path}/comments.json"

    # Save data to local file in newline delimited json format
    save_to_newline_delimited_json_file([subreddit_details], "subreddit.json")
    save_to_newline_delimited_json_file(posts, "posts.json")
    save_to_newline_delimited_json_file(comments, "comments.json")

    # Move files to GCS
    gcs_service = GCSService(Config.BUCKET)
    gcs_service.upload_file("subreddit.json", subreddit_file_path)
    gcs_service.upload_file("posts.json", posts_file_path)
    gcs_service.upload_file("comments.json", comments_file_path)

    return gcs_path


def parse_subreddit_details(subreddit):
    # Get subreddit details
    subreddit_details = {
        "subreddit_id": subreddit.id,
        "subreddit_name": subreddit.display_name,
        "subreddit_subscribers": subreddit.subscribers,
        "subreddit_type": subreddit.subreddit_type
    }
    return subreddit_details


def parse_post_details(raw_posts: list):
    # Prepare post details
    # Direct keys pulled from reddit post
    post_keys = [
        "id", "subreddit_id", "title", "selftext", "permalink", "ups", "downs", "num_comments", "num_crossposts", "total_awards_received", "locked", "stickied", "stickied", 
        "quarantine", "spoiler", "over_18", "link_flair_text", "link_flair_text_color", "link_flair_background_color", "created_utc", "approved_at_utc"
    ]
    post_data = []
    for post in raw_posts:
        try:
            post_dict = vars(post)
            post_record = {key: post_dict[key] for key in post_keys} 
            post_record["author_id"] = post_dict["author_fullname"]
            post_data.append(post_record)
        except Exception as e:
            log.error(vars(post))
            log.error(e)
    return post_data

    # # Create a DataFrame
    # columns = post_keys + ["author_id"]
    # post_df = pd.DataFrame(post_data, columns=columns)
    # return post_df


def parse_comment_details(reddit_service, raw_posts):

    # Prepare comments details
    comment_keys = [
        "id", "parent_id", "subreddit_id", "comment_type", "total_awards_received", "top_awarded_type", "likes", "body", "ups", "downs", 
        "stickied", "permalink", "created", "controversiality", "depth"
    ]
    comment_data = []
    for post in raw_posts:
        comments = reddit_service.get_comments(post)
        for comment in comments:
            comment_dict = vars(comment)
            comment_record = {key: comment_dict[key] for key in comment_keys} 
            if "author_fullname" in comment_dict.keys():
                comment_record["author_id"] = comment_dict["author_fullname"]
            else:
                comment_record["author_id"] = ""
            comment_data.append(comment_record)

    return comment_data

    # # Create a DataFrame
    # columns = comment_keys + ["author_id"]
    # comment_df = pd.DataFrame(comment_data, columns=columns)
    # print(f"Total comments found: {len(comment_df)}")


def save_to_newline_delimited_json_file(data, output_file):
    # Write the list of dictionaries to the NDJSON file
    with open(output_file, 'w') as f:
        for record in data:
            # Serialize dictionary to JSON
            json.dump(record, f)
            f.write('\n')
    return
