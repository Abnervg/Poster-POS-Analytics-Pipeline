#============================
# Imports
#============================
import boto3
import os
import json
from dotenv import load_dotenv

#===========================
# Logging Configuration
#===========================
import logging
logger = logging.getLogger(__name__)

#================================
# Poster POS ETL - Load Module
#================================

def save_raw_to_s3(data,date_from_str, date_str):
    """
    Saves the raw data to an S3 bucket.

    Args:
        data (list): The raw data to be saved.
        date_str (str): The date string used for naming the S3 object.

    Returns:
        None
    """

    # Load secret env vars
    load_dotenv()

    # Create the s3 client using credentials
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    bucket_name = os.getenv('S3_BUCKET')
    object_key = f'raw/receipts_{date_from_str}_{date_str}.json'

    try:
        # Convert data to JSON string
        json_data = json.dumps(data, indent=4)

        # Upload the JSON data to S3
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=json_data)
        logger.info(f'Successfully uploaded transformed data to s3://{bucket_name}/{object_key}')

    except Exception as e:
        logger(f'Error uploading data to S3: {e}')