"""
AWS Lambda handler for data analytics operations on BLS and DataUSA data.

Author: Sri Sudheera Chitipolu
Email: schitipolu34445@ucumberlands.edu
"""

import os
import boto3
import pandas as pd
import io
import json

# Initialize S3 client
s3 = boto3.client("s3")

# Get bucket name from environment variable
BUCKET_NAME = os.environ.get("BUCKET_NAME")

def load_csv_from_s3(key):
    """
    Load a CSV file from S3 into a pandas DataFrame.

    Args:
        key (str): The S3 key (path) to the CSV file

    Returns:
        pandas.DataFrame: The loaded CSV data

    Raises:
        boto3.exceptions.Boto3Error: If there's an issue accessing the S3 object
        pd.errors.ParserError: If there's an issue parsing the CSV
    """
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    body = obj['Body'].read()
    # Decompress if needed or directly load CSV
    # Here assuming uncompressed CSV for simplicity
    df = pd.read_csv(io.BytesIO(body))
    return df

def load_json_from_s3(key):
    """
    Load a JSON file from S3 into a pandas DataFrame.

    Args:
        key (str): The S3 key (path) to the JSON file

    Returns:
        pandas.DataFrame: The loaded JSON data, normalized into a flat table

    Raises:
        boto3.exceptions.Boto3Error: If there's an issue accessing the S3 object
        json.JSONDecodeError: If there's an issue parsing the JSON
    """
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    body = obj['Body'].read().decode('utf-8')
    data = json.loads(body)
    return pd.json_normalize(data.get("data", data))  # Handle both direct data and nested data structures

def handler(event, context):
    """
    AWS Lambda handler function for data analytics.

    This function loads BLS and DataUSA data from S3, performs data cleaning,
    and runs several analytics operations:
    1. Finds the best year per series_id based on summed values
    2. Calculates population statistics between 2013 and 2018
    3. Creates a joined report sample combining BLS and population data

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
    try:
        # Define S3 keys for the data files
        csv_key = "bls/pr/pr.data.0.Current"  # BLS data file
        json_key = "datausa/response.json"    # DataUSA population data file

        # Load data from S3
        df_csv = load_csv_from_s3(csv_key)
        df_json = load_json_from_s3(json_key)

        # --- Data Cleaning ---
        # Trim whitespaces in CSV columns & values
        df_csv.columns = df_csv.columns.str.strip()
        for col in df_csv.select_dtypes(include=['object']).columns:
            df_csv[col] = df_csv[col].str.strip()

        # Convert 'value' to numeric, coercing errors to NaN
        df_csv['value'] = pd.to_numeric(df_csv['value'], errors='coerce')

        # Convert 'year' to numeric for grouping
        df_csv['year'] = pd.to_numeric(df_csv['year'], errors='coerce')

        # --- Analytics 1: Best Year per series_id ---
        # Group by series_id and year, sum the values, and find the year with max value for each series
        grouped = df_csv.groupby(['series_id', 'year'])['value'].sum().reset_index()
        best_years = grouped.loc[grouped.groupby('series_id')['value'].idxmax()]
        print("Best year per series_id with summed value:")
        print(best_years.head())

        # --- Analytics 2: Population stats between 2013 and 2018 ---
        # Convert Year to numeric and filter for the desired range
        df_json['Year'] = pd.to_numeric(df_json['Year'], errors='coerce')
        pop_subset = df_json[(df_json['Year'] >= 2013) & (df_json['Year'] <= 2018)]

        # Calculate mean and standard deviation of population
        mean_pop = pop_subset['Population'].mean()
        std_pop = pop_subset['Population'].std()
        print(f"US Population Mean (2013-2018): {mean_pop}")
        print(f"US Population Std Dev (2013-2018): {std_pop}")

        # --- Analytics 3: Join sample report ---
        # Filter BLS data for a specific series and period
        filtered_csv = df_csv[(df_csv['series_id'] == 'PRS30006032') & (df_csv['period'] == 'Q01')]

        # Merge BLS data with population data on year
        merged = pd.merge(filtered_csv, df_json, left_on='year', right_on='Year', how='left')
        merged_report = merged[['series_id', 'year', 'period', 'value', 'Population']]
        print("Joined Report Sample:")
        print(merged_report.head())

        # Return success response
        return {
            "statusCode": 200,
            "body": "✅ Analytics completed and logged successfully."
        }

    except Exception as e:
        # Log the error and return error response
        print(f"Error in analytics handler: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"❌ Analytics failed: {str(e)}"
        }
