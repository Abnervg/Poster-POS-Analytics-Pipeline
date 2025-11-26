import boto3
import os
from dotenv import load_dotenv

#Load secret env vars
load_dotenv()

def test_connection():
    print('Starting connection')
    #Create the s3 client using credentials
    s3 = boto3.client(
        's3',
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name = os.getenv('AWS_REGION')
    )

    bucket_name = os.getenv('S3_BUCKET')

    try:
        #Try to list the files in the bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
        print(f'Success!! Connected to bucket: {bucket_name}')
        print('Folder structure verification:')
        #Check if we can see the folders created
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f" - {obj['Key']}")
        else:
            print('Bucket is empty or folders created via console are not showing up')
    
    except Exception as e:
        print(f'Error: {e}')

test_connection()

