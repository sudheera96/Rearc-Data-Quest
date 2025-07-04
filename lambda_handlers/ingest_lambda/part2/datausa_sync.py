"""
This module provides functionality to fetch population data from the DataUSA API
and upload it to an Amazon S3 bucket. It includes retry logic for API requests
and handles both population and comments data.

Author: Sri Sudheera Chitipolu
Email: schitipolu34445@ucumberlands.edu
"""

import requests
import boto3
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class DataUSASync:
    """
    A class to fetch population data from the DataUSA API and upload it to an S3 bucket.

    This class handles API requests with retry logic, data fetching, and S3 uploads.
    """

    def __init__(self, bucket_name, s3_prefix, user_agent=None):
        """
        Initialize the DataUSASync with S3 bucket information and request headers.

        Args:
            bucket_name (str): Name of the S3 bucket to upload data to
            s3_prefix (str): Prefix (folder path) within the S3 bucket
            user_agent (str, optional): User agent string for HTTP requests. 
                                       Defaults to a generic Firefox user agent if not provided.
        """
        self.api_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
        self.comments_api_url = "https://datausa.io/api/comments"
        self.bucket_name = bucket_name
        self.s3_prefix = s3_prefix.rstrip('/') + '/'
        self.s3_client = boto3.client('s3')

        # Headers with fallback User-Agent
        self.HEADERS = {
            'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0'
        }

        # Set up a session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,                    # Maximum number of retries
            backoff_factor=2,           # Exponential backoff factor between retries
            status_forcelist=[502, 503, 504],  # HTTP status codes to retry on
            allowed_methods=["GET"]     # HTTP methods to retry
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def fetch_data(self):
        """
        Fetch population data from the DataUSA API.

        This method makes an HTTP GET request to the DataUSA API with retry logic
        and returns the JSON response data.

        Returns:
            dict: The JSON response data from the API, or None if the request failed

        Note:
            This method handles exceptions internally and returns None on failure
            rather than raising exceptions, allowing for graceful degradation.
        """
        try:
            response = self.session.get(self.api_url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            print(f"✅ Fetched population data, size: {len(response.content)} bytes")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching population data: {e}")
            return None  # Don't raise, just return None to allow graceful exit

    def fetch_comments(self):
        """
        Fetch comments data from the DataUSA API.

        This method makes an HTTP GET request to the DataUSA API comments endpoint
        with retry logic and returns the JSON response data.

        Returns:
            dict: The JSON response data from the API, or None if the request failed

        Note:
            This method handles exceptions internally and returns None on failure
            rather than raising exceptions, allowing for graceful degradation.
        """
        try:
            response = self.session.get(self.comments_api_url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            print(f"✅ Fetched comments data, size: {len(response.content)} bytes")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching comments data: {e}")
            return None  # Don't raise, just return None to allow graceful exit

    def upload_to_s3(self, data, file_name="response.json"):
        """
        Upload the provided data to S3 as a JSON file.

        Args:
            data (dict): The data to upload to S3
            file_name (str, optional): The name of the file to create in S3.
                                      Defaults to "response.json".

        Raises:
            Exception: If the upload to S3 fails
        """
        s3_key = self.s3_prefix + file_name
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            print(f"📤 Uploaded data to s3://{self.bucket_name}/{s3_key}")
            print("✅ Upload complete.")
        except Exception as e:
            print(f"❌ Failed to upload to S3: {e}")
            raise

    def run(self):
        """
        Execute the complete data sync process.

        This method orchestrates the entire process:
        1. Fetch population data from the DataUSA API
        2. Upload the population data to S3 if the fetch was successful
        3. Fetch comments data from the DataUSA API
        4. Upload the comments data to S3 if the fetch was successful

        Returns:
            None
        """
        # Fetch and upload population data
        print("Fetching population API data...")
        population_data = self.fetch_data()
        if population_data:
            self.upload_to_s3(population_data, "response.json")
            print("✅ Successfully saved population data to S3.")
        else:
            print("⚠️ Skipping population data upload due to fetch failure.")

        # Fetch and upload comments data
        print("Fetching comments API data...")
        comments_data = self.fetch_comments()
        if comments_data:
            self.upload_to_s3(comments_data, "comments.json")
            print("✅ Successfully saved comments data to S3.")
        else:
            print("⚠️ Skipping comments data upload due to fetch failure.")
