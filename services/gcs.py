import json
from google.cloud import storage
from google.oauth2 import service_account
from utils.logging import get_logger


logger = get_logger(__name__)

class GCSService:
    def __init__(self, bucket_name: str, credentials_path: str=None):
        """
        Initialize the GCS Service with the necessary credentials and bucket name.
        """
        try:
            if credentials_path:
                self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
                self.client = storage.Client(credentials=self.credentials)
            else:
                self.client = storage.Client()
            self.bucket = self.client.get_bucket(bucket_name)
            logger.info("Connected to Google Cloud Storage.")
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise e

    def upload_file(self, source_file_name: dict, destination_blob_name: str):
        """
        Upload a JSON file to GCS.
        Args:
        - source_file_name: source file name.
        - destination_blob_name: GCS path to save the file (e.g., folder/file.json).
        """
        try:
            blob = self.bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_name)
            logger.info(f"data successfully uploaded to {destination_blob_name}.")
        except Exception as e:
            logger.error(f"Failed to upload JSON to GCS: {e}")
            raise e
