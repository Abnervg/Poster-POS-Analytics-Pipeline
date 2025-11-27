#=============================
# ETL - EXTRACT MODULE
#=============================

#=============================
#Imports
#=============================
import logging
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
from etl.s3_client import S3Client
from datetime import date, timedelta
from pathlib import Path
import json

#=============================
# Load secret env vars
#=============================
load_dotenv()

#=============================
#Transformations
#=============================

def extract_from_s3(object_key):
    """
    Extracts JSON data from S3 using the provided S3 client.

    Args:
        s3_client (S3Client): An instance of the S3Client class.
        object_key (str): The S3 object key (path in the bucket).
    Returns:
        dict or list: The JSON data extracted from S3.
    """
    s3 = S3Client()
    try:
        data = s3.read_json(object_key)
        logging.info(f'Successfully extracted data from s3://{s3.bucket_name}/{object_key}')

        #Validate data structure
        if 'response' in data and isinstance(data['response'], list):
            transaction_headers = data['response']
            transaction_ids = [header['transaction_id'] for header in transaction_headers]
            logging.info(f'Fetched {len(transaction_ids)} transaction IDs.')
            return data
        else:
            logging.warning('No transaction headers found in the response.')
            return []
        
        

    except Exception as e:
        logging.error(f'Error extracting data from S3: {e}')
        return None

def transformations(data):
    """
    Flattens the transactions data structure and cleans data types.

    Args:
        transactions (json): Data for a list of transactions.
    Returns:
        pd.DataFrame: Transformed data as a pandas DataFrame.
    """

    try:

        #Check if data is wraped by 'response' key and unwrap it
        if 'response' in data:
            data = data['response']
        
        #Filter transactions with products only
        clean_data = [
        t for t in data 
        if 'products' in t and isinstance(t['products'], list) and len(t['products']) > 0]

        if not clean_data:
            logging.info('No transactions with products found for transformation.')
            return

        # Normalize JSON data to flatten nested structures
        df = pd.json_normalize(clean_data, record_path=['products'], meta=[
            'transaction_id',
            'date_close',
            'spot_id',
            'table_name',
            'payed_sum'
        ],
        meta_prefix='receipt_',
        errors='ignore'
        )

        #Data type conversions
        #Convert string to numbers
        numeric_fields = ['product_price', 'num', 'product_profit', 'payed_sum']
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        #Convert Unix timestamp to readable date (Poster uses miliseconds strings)
        if 'date_close' in df.columns:
            df['date_close'] = pd.to_datetime(df['date_close'])
            df['timestamp'] = pd.to_datetime(df['date_close'], unit='ms')

        return df

    except Exception as e:
        logging.error(f'Error during transformation: {e}')
        return []
    
#=============================
# Main transform function
#=============================

def transform(date_from, date_to):

    # Define S3 object key for raw data
    object_key = f'raw/sales/sales_{date_from}_{date_to}.json' 

    # Step 1: Extract raw data from S3
    raw_data = extract_from_s3(object_key)

    if not raw_data:
        logging.info('No raw data to transform. Exiting.')
        return

    # Step 2: Transform data
    transformed_df = transformations(raw_data)

    if transformed_df.empty:
        logging.info('Transformation resulted in empty DataFrame. Exiting.')
        return
    
    logging.info('Transformation completed.')

    # Step 3: Save transformed data back to S3 in Parquet format
    s3 = S3Client()

    if date_from == date_to:
        target_key = f'curated/sales/sales_{date_from}.parquet'
    else:
        target_key = f'curated/sales/sales_{date_from}_{date_to}.parquet'
    s3.upload_parquet(transformed_df, target_key)
    logging.info(f'Transformed data uploaded to s3://{s3.bucket_name}/{target_key}')
    return     
    
