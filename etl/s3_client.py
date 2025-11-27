#=================================
# Import necessary modules
#=================================
import boto3
import os
from dotenv import load_dotenv
import json
import io
import pandas as pd
import logging
#=================================

#=================================
# S3 Client for Poster POS ETL
#=================================

#Initialize logging
logger = logging.getLogger(__name__)

#Define S3 client

class S3Client:
    def __init__(self):
        # Load secret env vars
        load_dotenv()

        #Create the s3 client using credentials
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        self.bucket_name = os.getenv('S3_BUCKET')

    def upload_json(self, data, object_key):
        """
        Uploads JSON data to the specified S3 bucket.

        Args:
            data (dict or list): The data to be uploaded.
            object_key (str): The S3 object key (path in the bucket).

        Returns:
            None
        """
        try:
            json_data = json.dumps(data, indent=4)
            logger.info(f'Data has {len(data)} records to upload to S3.')
            self.s3.put_object(Bucket=self.bucket_name, Key=object_key, Body=json_data)
            logger.info(f'Successfully uploaded data to s3://{self.bucket_name}/{object_key}')
        except Exception as e:
            logger.error(f'Error uploading data to S3: {e}')

    def read_json(self, object_key):
        """
        Reads JSON data from the specified S3 bucket.

        Args:
            object_key (str): The S3 object key (path in the bucket).
        Returns:
            dict or list: The JSON data read from S3.
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=object_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            logger.info(f'Successfully read data from s3://{self.bucket_name}/{object_key}')
            return data
        
        except:
            logger.info(f'No data found at s3 for key {object_key}')
            return None
    
    def upload_parquet(self, df, object_key):
        """
        Uploads a pandas DataFrame as a Parquet file to the specified S3 bucket.

        Args:
            df (pd.DataFrame): The DataFrame to be uploaded.
            object_key (str): The S3 object key (path in the bucket).
        Returns:
            None
        """
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            self.s3.put_object(Bucket=self.bucket_name, Key=object_key, Body=buffer)
            logger.info(f'Successfully uploaded DataFrame as Parquet to s3://{self.bucket_name}/{object_key}')
        except Exception as e:
            logger.error(f'Error uploading Parquet to S3: {e}')