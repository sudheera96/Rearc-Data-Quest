"""
Main module for Rearc Data Quest project.

Author: Sri Sudheera Chitipolu
Email: schitipolu34445@ucumberlands.edu
"""

from lambda_handlers.ingest_lambda.part1.bls_s3_sync import BLSSync
from lambda_handlers.ingest_lambda.part2.datausa_sync import DataUSASync

def main():
    """
    Main function to execute the data synchronization process.

    This function initializes and runs two data synchronization processes:
    1. BLSSync - Synchronizes Bureau of Labor Statistics data to an S3 bucket
    2. DataUSASync - Fetches population data from DataUSA API and uploads to S3

    The function configures the necessary parameters for both sync processes,
    including the S3 bucket name, prefixes, and user agent.

    Returns:
        None
    """
    # Configuration parameters
    bucket_name = "bls-data-sync-sri"  # S3 bucket name
    s3_prefix = "bls/pr/"  # Prefix for BLS data in S3
    user_agent = "Sri (schitipolu34445@ucumberlands.edu) - Rearc Data Quest"  # User agent for HTTP requests
    s3_prefix_part2 = 'datausa'  # Prefix for DataUSA data in S3

    # Step 1: Sync BLS data to S3
    syncer = BLSSync(bucket_name, s3_prefix, user_agent)
    syncer.sync()

    # Step 2: Fetch DataUSA API data and upload to S3
    sync_part2 = DataUSASync(bucket_name, s3_prefix_part2, user_agent)
    sync_part2.run()


if __name__ == "__main__":
    main()
