import os

"""
InfraStack: AWS CDK Infrastructure Stack for Data Processing Pipeline

This module defines the AWS CDK infrastructure stack for a data processing pipeline that:
1. Ingests data from BLS (Bureau of Labor Statistics) and DataUSA sources
2. Stores the data in an S3 bucket
3. Processes the data using Lambda functions
4. Orchestrates the workflow using SQS queues and EventBridge rules

The stack creates and configures the following AWS resources:
- References an existing S3 bucket for data storage
- Creates an SQS queue for message processing
- Defines Lambda functions for data ingestion and analytics
- Sets up Lambda layers for dependencies
- Configures event triggers and schedules

Author: Sri Sudheera Chitipolu
Email: schitipolu34445@ucumberlands.edu
"""

from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda_event_sources as lambda_events,
)
from constructs import Construct

class InfraStack(Stack):
    """
    AWS CDK Stack that defines the infrastructure for the data processing pipeline.

    This stack creates and configures all the necessary AWS resources for the
    data ingestion and analytics pipeline, including Lambda functions, SQS queues,
    S3 event notifications, and scheduled events.
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """
        Initialize the InfraStack with all required AWS resources.

        Args:
            scope (Construct): The parent construct
            construct_id (str): The ID of this stack
            **kwargs: Additional keyword arguments passed to the Stack constructor
        """
        super().__init__(scope, construct_id, **kwargs)

        # Reference existing S3 bucket
        # This bucket is used to store both the raw data from BLS and DataUSA sources
        # and the processed analytics results
        bucket = s3.Bucket.from_bucket_name(self, "DataBucket", "bls-data-sync-sri")

        # Create SQS queue for asynchronous processing
        # This queue is triggered by S3 events when new DataUSA data is uploaded
        # and then triggers the analytics Lambda function
        queue = sqs.Queue(
            self, "DataProcessingQueue",
            visibility_timeout=Duration.minutes(5)  # Match the Lambda timeout to prevent message reprocessing
        )

        # Define paths to Lambda function code and layers
        # These paths are relative to the current file and point to the directories
        # containing the Lambda function code and dependencies

        # Path to the ingest Lambda function code (BLS and DataUSA data ingestion)
        lambda_ingest_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..', 'lambda_handlers', 'ingest_lambda')
        )

        # Path to the analytics Lambda function code (data processing and analysis)
        lambda_analytics_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..', 'lambda_handlers', 'analytics_lambda')
        )

        # Path to the requests library Lambda layer
        # This layer provides the requests HTTP library for the Lambda functions
        lambda_layers_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..', 'lambda_layers', 'requests_layer')
        )

        # Path to the BeautifulSoup library Lambda layer
        # This layer provides the BS4 library for HTML parsing in the Lambda functions
        lambda_bs4_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..', 'python')
        )

        # Create Lambda layers for shared dependencies
        # Lambda layers allow us to package and reuse code across multiple Lambda functions

        # Create a layer for the requests HTTP library
        # This layer is used by both Lambda functions to make HTTP requests
        requests_layer = _lambda.LayerVersion(
            self, "RequestsLayer",
            code=_lambda.Code.from_asset(os.path.abspath(lambda_layers_path)),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],  # Specify compatible runtime
            description="Layer with requests HTTP library for Lambda functions",
        )

        # Create a layer for the BeautifulSoup library
        # This layer is used for HTML parsing in the Lambda functions
        bs4_layer = _lambda.LayerVersion(
            self, "RequestsBS4Layer",
            code=_lambda.Code.from_asset(os.path.abspath(lambda_bs4_path)),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],  # Specify compatible runtime
            description="Layer with BeautifulSoup HTML parsing library"
        )

        # Create Lambda function for data ingestion (Part1 + Part2)
        # This Lambda function is responsible for:
        # 1. Fetching data from BLS (Bureau of Labor Statistics) API
        # 2. Fetching data from DataUSA API
        # 3. Storing the fetched data in the S3 bucket
        ingest_lambda = _lambda.Function(
            self, "IngestLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,  # Use Python 3.11 runtime
            handler="ingest_handler.handler",     # Entry point for the Lambda function
            code=_lambda.Code.from_asset(lambda_ingest_path),  # Code location
            layers=[requests_layer, bs4_layer],   # Include dependency layers
            description="Lambda function that ingests data from BLS and DataUSA APIs"
        )

        # Create Lambda function for data analytics (Part3)
        # This Lambda function is responsible for:
        # 1. Loading data from the S3 bucket
        # 2. Performing data cleaning and transformation
        # 3. Running analytics operations on the data
        # 4. Generating reports and insights
        analytics_lambda = _lambda.Function(
            self, "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,  # Use Python 3.11 runtime
            handler="analytics_handler.handler",  # Entry point for the Lambda function
            code=_lambda.Code.from_asset(lambda_analytics_path),  # Code location
            timeout=Duration.minutes(5),          # Set timeout to 5 minutes for longer processing
            layers=[requests_layer, bs4_layer],   # Include dependency layers
            environment={
                "BUCKET_NAME": bucket.bucket_name,  # Pass bucket name as environment variable
            },
            description="Lambda function that performs analytics on ingested data"
        )
        # Configure event triggers and workflow orchestration

        # Set up S3 event notification to trigger SQS when new JSON is uploaded to 'datausa/' prefix
        # This creates an event-driven workflow where new data uploads automatically trigger processing
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,          # Trigger on object creation events
            s3n.SqsDestination(queue),            # Send notification to SQS queue
            s3.NotificationKeyFilter(prefix="datausa/")  # Only for objects in 'datausa/' prefix
        )

        # Configure SQS queue as an event source for the Analytics Lambda
        # This allows the analytics function to process messages from the queue asynchronously
        analytics_lambda.add_event_source(
            lambda_events.SqsEventSource(queue)   # Connect SQS queue to Lambda function
        )

        # Schedule daily execution of the Ingest Lambda using EventBridge
        # This ensures data is regularly fetched from the source APIs and kept up-to-date
        rule = events.Rule(
            self, "DailyIngestSchedule",
            schedule=events.Schedule.rate(Duration.days(1)),  # Run once per day
            description="Daily schedule for data ingestion from BLS and DataUSA"
        )
        rule.add_target(targets.LambdaFunction(ingest_lambda))  # Set Lambda as the target
