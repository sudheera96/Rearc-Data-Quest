"""
This module provides functionality to synchronize data files from the Bureau of Labor Statistics (BLS)
to an Amazon S3 bucket. It handles fetching file lists, comparing file versions using MD5 hashes,
and performing efficient uploads/deletions to keep the S3 bucket in sync with the BLS source.

Author: Sri Sudheera Chitipolu
Email: schitipolu34445@ucumberlands.edu
"""

import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import hashlib

class BLSSync:
    """
    A class that handles synchronization of BLS data files to an S3 bucket.

    This class scrapes the BLS website for data files, compares them with existing files
    in the S3 bucket, and performs necessary uploads and deletions to keep the S3 bucket
    in sync with the BLS source.
    """
    def __init__(self, bucket_name, prefix, user_agent):
        """
        Initialize the BLSSync object with S3 bucket details and request headers.

        Args:
            bucket_name (str): Name of the S3 bucket to sync files to
            prefix (str): Prefix path in the S3 bucket where files will be stored
            user_agent (str): User agent string to use in HTTP requests to BLS
        """
        self.SOURCE_URL = "https://download.bls.gov/pub/time.series/pr/"
        self.BUCKET_NAME = bucket_name
        self.S3_PREFIX = prefix
        self.HEADERS = {'User-Agent': user_agent}
        print(self.HEADERS)
        self.s3 = boto3.client("s3")

    def get_bls_file_list(self):
        """
        Scrape the BLS website to get a list of available data files.

        This method fetches the BLS directory page, parses the HTML to find all file links,
        and returns a list of complete URLs to each file.

        Returns:
            list: A list of URLs to BLS data files
        """
        response = requests.get(self.SOURCE_URL, headers=self.HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        files = []

        for link in soup.find_all("a"):
            href = link.get("href")
            if not href or href.startswith("../"):
                continue  # Skip parent directory links
            filename = href.split("/")[-1]
            if filename:  # skip blank filenames
                files.append(urljoin(self.SOURCE_URL, href))

        print(f"🔍 Found {len(files)} files on BLS site.")
        for f in files:
            print("📄 File URL:", f)
        return files

    def get_s3_file_map(self):
        """
        Retrieve a mapping of files currently in the S3 bucket.

        This method lists all objects in the S3 bucket with the specified prefix
        and creates a dictionary mapping filenames to their ETag values (MD5 hashes).

        Returns:
            dict: A dictionary mapping filenames to their ETag values
        """
        file_map = {}
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.BUCKET_NAME, Prefix=self.S3_PREFIX):
            for obj in page.get("Contents", []):
                name = obj["Key"].replace(self.S3_PREFIX, "")
                file_map[name] = obj["ETag"].strip('"')  # Remove quotes from ETag
        return file_map

    def hash_streamed_file(self, file_stream):
        """
        Calculate MD5 hash of a file stream.

        This method efficiently calculates the MD5 hash of a file by processing
        it in chunks, which is memory-efficient for large files.

        Args:
            file_stream (requests.Response): A requests response object with streaming enabled

        Returns:
            str: Hexadecimal string representation of the MD5 hash
        """
        hash_md5 = hashlib.md5()
        for chunk in file_stream.iter_content(chunk_size=4096):
            hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def sync(self):
        """
        Perform the synchronization between BLS website and S3 bucket.

        This is the main method that orchestrates the entire sync process:
        1. Retrieves the list of files from BLS website
        2. Gets the current state of files in the S3 bucket
        3. For each BLS file, checks if it needs to be uploaded (new or changed)
        4. Removes files from S3 that no longer exist on the BLS website

        The method uses MD5 hashing to determine if files have changed and
        only uploads files that are new or have been modified.

        Returns:
            None
        """
        print("🚀 Starting sync from BLS to S3...\n")
        bls_urls = self.get_bls_file_list()
        s3_files = self.get_s3_file_map()
        current_filenames = set()

        for url in bls_urls:
            filename = url.split("/")[-1]
            current_filenames.add(filename)
            print(f"\n🔎 Checking file: {filename}")

            # Step 1: Get file hash
            try:
                r_hash = requests.get(url, headers=self.HEADERS, stream=True)
                r_hash.raise_for_status()
                file_hash = self.hash_streamed_file(r_hash)
            except Exception as e:
                print(f"❌ Failed to fetch or hash {filename}: {e}")
                continue

            # Step 2: Upload if new or changed
            if filename not in s3_files or s3_files[filename] != file_hash:
                print(f"🔄 Downloading {url}")
                try:
                    r_upload = requests.get(url, headers=self.HEADERS, stream=True)
                    r_upload.raise_for_status()
                    print(f"✅ Downloaded {filename}, size: {r_upload.headers.get('Content-Length')} bytes")

                    s3_key = f"{self.S3_PREFIX}{filename}"
                    print(f"📤 Uploading to S3 key: {s3_key}")
                    self.s3.upload_fileobj(r_upload.raw, self.BUCKET_NAME, s3_key)
                    print(f"✅ Successfully uploaded {filename}")
                except Exception as e:
                    print(f"❌ ERROR uploading {filename}: {e}")
            else:
                print(f"⏭️ Skipping {filename}, no changes detected.")

        # Step 3: Remove files no longer in BLS
        for old_file in s3_files:
            if old_file not in current_filenames:
                print(f"🗑️ Deleting {old_file} from S3 (no longer in source).")
                self.s3.delete_object(Bucket=self.BUCKET_NAME, Key=f"{self.S3_PREFIX}{old_file}")

        print("\n✅ Sync complete.")
