from google.cloud import bigquery
from google.oauth2 import service_account
from utils.logging import get_logger


logger = get_logger(__name__)

class BigQueryService:

    def __init__(self, project_id: str=None,  credentials_path: str=None):
        """
        Initialize the BigQuery Service with the necessary credentials and project ID.
        """
        try:
            if credentials_path:
                self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
                self.client = bigquery.Client(credentials=self.credentials, project=project_id)
            else:
                if project_id is None:
                    self.client = bigquery.Client()
                else:
                    self.client = bigquery.Client(project=project_id)
            logger.info("Connected to BigQuery.")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise e

    def create_table(self, dataset_id: str, table_id: str, schema: list):
        """
        Creates a new table in BigQuery.
        Args:
        - dataset_id: The BigQuery dataset ID.
        - table_id: The ID for the new table.
        - schema: A list of bigquery.SchemaField objects.
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)

        try:
            self.client.create_table(table)
            logger.info(f"Table {table_id} created successfully in dataset {dataset_id}.")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise e

    def insert_rows(self, dataset_id: str, table_id: str, rows: list):
        """
        Inserts rows into an existing BigQuery table.
        Args:
        - dataset_id: The BigQuery dataset ID.
        - table_id: The ID of the table.
        - rows: A list of dictionaries, where each dictionary represents a row to insert.
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        
        try:
            errors = self.client.insert_rows_json(table_ref, rows)
            if errors:
                logger.error(f"Encountered errors while inserting rows: {errors}")
            else:
                logger.info(f"Inserted {len(rows)} rows into {dataset_id}.{table_id}.")
        except Exception as e:
            logger.error(f"Failed to insert rows into table: {e}")
            raise e

    def query(self, query: str):
        """
        Executes a SQL query on BigQuery.
        Args:
        - query: SQL query string.
        Returns:
        - A pandas DataFrame containing the query results.
        """
        try:
            query_job = self.client.query(query)
            query_job.result()
            logger.info("Query executed successfully.")
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise e

    def query_to_df(self, query: str):
        """
        Executes a SQL query on BigQuery.
        Args:
        - query: SQL query string.
        Returns:
        - A pandas DataFrame containing the query results.
        """
        try:
            query_job = self.client.query(query)
            df = query_job.to_dataframe()
            logger.info("Query executed successfully.")
            return df
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise e


    def load_json_from_gcs_to_bq(self, gcs_uri: str, dataset_id: str, table_id: str, mode: str='append', file_format: str="json"):
        """
        Load JSON data from GCS to BigQuery, replacing existing data.

        Args:
            gcs_uri (str): GCS URI (e.g., gs://bucket_name/path/to/file.json)
            dataset_id (str): ID of the BigQuery dataset.
            table_id (str): ID of the BigQuery table.
        
        Returns:
            None
        """
        try:
            # Set the table reference
            table_ref = self.client.dataset(dataset_id).table(table_id)

            if mode == "append":
                bq_mode = bigquery.WriteDisposition.WRITE_APPEND
            elif mode == "replace":
                bq_mode = bigquery.WriteDisposition.WRITE_TRUNCATE
            else:
                bq_mode = bigquery.WriteDisposition.WRITE_EMPTY
            
            if file_format == "json":
                source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            elif file_format == "csv":
                source_format = bigquery.SourceFormat.CSV
            elif file_format == "avro":
                source_format = bigquery.SourceFormat.AVRO
            elif file_format == "parquet":
                source_format = bigquery.SourceFormat.PARQUET

            # Define the job configuration
            job_config = bigquery.LoadJobConfig(
                source_format=source_format,
                write_disposition=bq_mode,
                autodetect=True
            )

            # Load the data from GCS
            load_job = self.client.load_table_from_uri(
                gcs_uri, 
                table_ref, 
                job_config=job_config
            )
            
            # Wait for the load job to complete
            load_job.result()
            logger.info(f"Data loaded successfully from {gcs_uri} to {dataset_id}.{table_id}.")

        except Exception as e:
            logger.error(f"Failed to load data from GCS to BigQuery: {e}")
            raise e
