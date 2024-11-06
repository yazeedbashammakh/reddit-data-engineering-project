
from services.bigquery import BigQueryService
from utils.config import Config


def load_to_bq_staging(data_name:str, **kwargs):
    gcs_path = kwargs['ti'].xcom_pull(task_ids='fetch_data_task')

    gcs_uri = f"gs://{Config.BUCKET}/{gcs_path}/{data_name}.json"
    dataset_id = Config.STAGE_DATASET_ID
    table_id = Config.TABLES[data_name]

    bq_service = BigQueryService()
    bq_service.load_json_from_gcs_to_bq(gcs_uri=gcs_uri, dataset_id=dataset_id, table_id=table_id, mode="replace")


def load_to_bq_subreddit_final():
    source_table = f"{Config.GCP_PROJECT}.{Config.STAGE_DATASET_ID}.{Config.TABLES['subreddit']}"
    target_table = f"{Config.GCP_PROJECT}.{Config.FINAL_DATASET_ID}.{Config.TABLES['subreddit']}"
    query = f"""
        MERGE INTO `{target_table}` as T
        USING `{source_table}` as S 
        ON (
        T.subreddit_id = S.subreddit_id
        )
        WHEN NOT MATCHED THEN
        INSERT 
            (subreddit_id, subreddit_name, subreddit_subscribers, subreddit_type)
        VALUES (
            S.subreddit_id, S.subreddit_name, S.subreddit_subscribers, S.subreddit_type
        )

        WHEN MATCHED THEN
        UPDATE SET
            T.subreddit_name = S.subreddit_name, 
            T.subreddit_subscribers = S.subreddit_subscribers, 
            T.subreddit_type = S.subreddit_type
        ;

    """
    bq_service = BigQueryService()
    bq_service.query(query)

    


def load_to_bq_posts_final():
    source_table = f"{Config.GCP_PROJECT}.{Config.STAGE_DATASET_ID}.{Config.TABLES['posts']}"
    target_table = f"{Config.GCP_PROJECT}.{Config.FINAL_DATASET_ID}.{Config.TABLES['posts']}"
    query = f"""
        MERGE INTO `{target_table}` as T
        USING `{source_table}` as S 
        ON (
        T.id = S.id
            and
        T.subreddit_id = S.subreddit_id
        )
        WHEN NOT MATCHED THEN
        INSERT 
            (id, subreddit_id, author_id, title, selftext, permalink, num_comments, ups, downs, total_awards_received, num_crossposts, link_flair_text, link_flair_text_color, link_flair_background_color, stickied, over_18, spoiler, locked, quarantine, created_utc, approved_at_utc)
        VALUES (
            S.id,
            S.subreddit_id,
            S.author_id,
            S.title,
            S.selftext,
            S.permalink,
            S.num_comments,
            S.ups,
            S.downs,
            S.total_awards_received,
            S.num_crossposts,
            S.link_flair_text,
            S.link_flair_text_color,
            S.link_flair_background_color,
            S.stickied,
            S.over_18,
            S.spoiler,
            S.locked,
            S.quarantine,
            TIMESTAMP_SECONDS(cast(S.created_utc as int64)),
            TIMESTAMP_SECONDS(cast(S.approved_at_utc as int64))
        )

        WHEN MATCHED THEN
        UPDATE SET
            T.author_id = S.author_id,
            T.title = S.title,
            T.selftext = S.selftext,
            T.permalink = S.permalink,
            T.num_comments = S.num_comments,
            T.ups = S.ups,
            T.downs = S.downs,
            T.total_awards_received = S.total_awards_received,
            T.num_crossposts = S.num_crossposts,
            T.link_flair_text = S.link_flair_text,
            T.link_flair_text_color = S.link_flair_text_color,
            T.link_flair_background_color = S.link_flair_background_color,
            T.stickied = S.stickied,
            T.over_18 = S.over_18,
            T.spoiler = S.spoiler,
            T.locked = S.locked,
            T.quarantine = S.quarantine,
            T.created_utc = TIMESTAMP_SECONDS(cast(S.created_utc as int64)),
            T.approved_at_utc = TIMESTAMP_SECONDS(cast(S.approved_at_utc as int64))
        ;
    """
    bq_service = BigQueryService()
    bq_service.query(query)


def load_to_bq_comments_final():
    source_table = f"{Config.GCP_PROJECT}.{Config.STAGE_DATASET_ID}.{Config.TABLES['comments']}"
    target_table = f"{Config.GCP_PROJECT}.{Config.FINAL_DATASET_ID}.{Config.TABLES['comments']}"
    query = f"""
        MERGE INTO `{target_table}` as T
        USING `{source_table}` as S 
        ON (
        T.id = S.id
            and
        T.subreddit_id = S.subreddit_id
        )
        WHEN NOT MATCHED THEN
        INSERT 
            (id, subreddit_id, author_id, parent_id, depth, body, comment_type, permalink, ups, downs, likes, total_awards_received, top_awarded_type, stickied, controversiality, created)
        VALUES (
            S.id,
            S.subreddit_id, 
            S.author_id,
            S.parent_id, 
            S.depth,
            S.body, 
            S.comment_type,
            S.permalink,
            S.ups, 
            S.downs,
            S.likes,
            S.total_awards_received,
            S.top_awarded_type, 
            S.stickied, 
            S.controversiality, 
            TIMESTAMP_SECONDS(cast(S.created as int64))
        )

        WHEN MATCHED THEN
        UPDATE SET
            T.author_id = S.author_id,
            T.parent_id = S.parent_id, 
            T.depth = S.depth,
            T.body = S.body, 
            T.comment_type = S.comment_type,
            T.permalink = S.permalink,
            T.ups = S.ups, 
            T.downs = S.downs,
            T.likes = S.likes,
            T.total_awards_received = S.total_awards_received,
            T.top_awarded_type = S.top_awarded_type, 
            T.stickied = S.stickied, 
            T.controversiality = S.controversiality, 
            T.created = TIMESTAMP_SECONDS(cast(S.created as int64))
        ;
    """
    bq_service = BigQueryService()
    bq_service.query(query)



def last_month_aggregated_metrics():
    agg_table = f"{Config.GCP_PROJECT}.{Config.FINAL_DATASET_ID}.{Config.TABLES['last_month_agg']}"
    post_table = f"{Config.GCP_PROJECT}.{Config.FINAL_DATASET_ID}.{Config.TABLES['posts']}"
    comment_table = f"{Config.GCP_PROJECT}.{Config.FINAL_DATASET_ID}.{Config.TABLES['comments']}"
    query = f"""
    CREATE OR REPLACE TABLE `{agg_table}` 
        AS 
    WITH post_metrics AS 
    (
        SELECT 
            subreddit_id,
            author_id,
            COUNT(distinct posts.id) / 4 AS post_frequency,
            AVG(posts.ups - posts.downs) AS avg_post_karma,
            SUM(posts.num_comments) / COUNT(distinct posts.id) AS avg_comments_per_post,
        FROM `{post_table}` posts
        WHERE
            cast(created_utc as date) between DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) and CURRENT_DATE
        GROUP BY 1,2
    ),
    comments_metrics as (
        SELECT 
            subreddit_id,
            author_id,
            COUNT(distinct c.id) / 4 AS comment_frequency,
            AVG(c.ups - c.downs) AS avg_comment_karma,
            SUM(total_awards_received) as total_awards,

        FROM `{comment_table}` c
        WHERE
            cast(created as date) between DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY) and CURRENT_DATE
        GROUP BY 1,2
    ),
    all_metrics as (
        SELECT
            coalesce(p.subreddit_id, c.subreddit_id) as subreddit_id,
            coalesce(p.author_id, c.author_id) as author_id,
            coalesce(post_frequency, 0) as post_frequency,
            coalesce(comment_frequency, 0) as comment_frequency,
            coalesce(avg_post_karma, 0) as avg_post_karma,
            coalesce(avg_comment_karma, 0) as avg_comment_karma,
            coalesce(avg_comments_per_post, 0) as avg_comments_per_post,
            coalesce(total_awards, 0) as total_awards
        FROM 
            post_metrics p
                FULL JOIN
            comments_metrics c
                ON
            (p.subreddit_id = c.subreddit_id
                and
            p.author_id = c.author_id)
    )

    SELECT 
        *,
        (post_frequency + comment_frequency) as frequency_score,
        (avg_post_karma + avg_comment_karma + avg_comments_per_post + total_awards) as engagement_score, 
        CURRENT_DATE() as load_date
    FROM all_metrics
    ORDER BY engagement_score + frequency_score DESC
    ;
    """
    bq_service = BigQueryService()
    bq_service.query(query)

