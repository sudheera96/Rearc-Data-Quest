"""
AWS Lambda handler for data ingestion from BLS and DataUSA sources.

Author: Sri Sudheera Chitipolu
Email: schitipolu34445@ucumberlands.edu
"""

from part1.bls_s3_sync import BLSSync
from part2.datausa_sync import DataUSASync

def handler(event, context):
    """
    AWS Lambda handler function for data ingestion.

    This function is triggered by AWS Lambda and orchestrates the data synchronization
    process from both BLS and DataUSA sources to an S3 bucket. It's designed to be
    run on a schedule to keep the data up-to-date.

    Args:
        event (dict): The event dict that contains the parameters passed when the function
                     is invoked (not used in this implementation)
        context (LambdaContext): The context object that provides methods and properties
                                about the invocation, function, and execution environment

    Returns:
        dict: A response object containing a statusCode and message body
              - 200 status code for successful execution
              - 500 status code if an error occurs during execution
    """
    # Configuration parameters
    bucket_name = "bls-data-sync-sri"  # S3 bucket name
    s3_prefix = "bls/pr/"  # Prefix for BLS data in S3
    user_agent = "Sri (schitipolu34445@ucumberlands.edu) - Rearc Data Quest"  # User agent for HTTP requests
    s3_prefix_part2 = 'datausa'  # Prefix for DataUSA data in S3

    try:
        # Step 1: Sync BLS data to S3
        syncer = BLSSync(bucket_name, s3_prefix, user_agent)
        syncer.sync()

        # Step 2: Fetch DataUSA API data and upload to S3
        sync_part2 = DataUSASync(bucket_name, s3_prefix_part2, user_agent)
        sync_part2.run()

        # Return success response
        return {
            "statusCode": 200,
            "body": "✅ BLS and DataUSA sync completed successfully"
        }

    except Exception as e:
        # Return error response if any exception occurs
        return {
            "statusCode": 500,
            "body": f"❌ Sync failed: {str(e)}"
        }
