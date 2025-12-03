#=============================
# ETL - EXTRACT MODULE
#=============================

#=============================
# Imports
#=============================
import logging
from dotenv import load_dotenv
import pandas as pd
from etl.s3_client import S3Client
from datetime import datetime

#=============================
# Load secret env vars
#=============================
load_dotenv()

#=============================
# Transformations
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
        if data is None:
            date = object_key.split('_')[-1].split('.')[0]
            logging.info(f'No data found for date {date}')
            return None
        else:
            logging.info(f'✅Successfully extracted data from s3 key: {object_key}')

        #Validate data structure
        if 'response' in data and isinstance(data['response'], list):
            transaction_headers = data['response']
            transaction_ids = [header['transaction_id'] for header in transaction_headers]
            logging.info(f'Fetched {len(transaction_ids)} transaction IDs.')
            return data
        else:
            logging.warning('❌FATAL! No transaction headers found in the response.❌')
            logging.warning('❌DATA STRUCTURED COMPROMISED❌')
            return None

    except Exception as e:
        logging.error(f'❌Error extracting data from S3: {e}')
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
            return None

        # Normalize JSON data to flatten nested structures
        df = pd.json_normalize(
        clean_data, 
        record_path=['products'], 
        meta=[
            # --- IDENTIFIERS ---
            'transaction_id',
            'spot_id',
            'table_name',
            'user_id',          # Waiter
            'client_id',        # Customer (CRM)
            
            # --- TIME ---
            'date_start',       # Table Open Time
            'date_close',       # Payment Time
            
            # --- FINANCIALS (RECEIPT LEVEL) ---
            'payed_sum',        # Total Bill
            'pay_type',         # Cash/Card/etc
            'discount',         # Leakage
            'bonus',            # Loyalty Points Used
            'tip_sum',          # Staff Tips
            'tax_sum',          # Tax (VAT)
            
            # --- OPERATIONS ---
            'guests_count',     # Capacity
            'service_mode',     # 1=Dine-in, 2=Takeaway, etc.
            'status'            # 2=Closed, 3=Deleted
        ],
        meta_prefix='receipt_',
        errors='ignore'
    )

        # --- DATA TYPE CLEANING ---
    
        # 1. Numeric Conversion
        # We include product_cost here so it becomes a number (0.00), not a string
        numeric_fields = [
            # Product Level
            'product_price', 'num', 'product_profit', 
            'product_cost', 'product_cost_netto', # <--- FUTURE PROOFING
            
            # Receipt Level
            'receipt_payed_sum', 'receipt_discount', 'receipt_tip_sum', 
            'receipt_guests_count', 'receipt_tax_sum', 'receipt_bonus'
        ]
        
        for col in numeric_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # 2. ID Columns (Ensure they stay strings to avoid ".0")
        id_cols = ['receipt_transaction_id', 'receipt_user_id', 'receipt_client_id', 'modification_id', 'product_id']
        for col in id_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).replace('nan', '0').replace('None', '0')

        # 3. Timestamp Conversion
        time_cols = ['receipt_date_close', 'receipt_date_start']
        for col in time_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    except Exception as e:
        logging.error(f'❌Error during transformation: {e}')
        return None
    
#=============================
# Main transform function
#=============================

def transform(date_from, date_to):

    #Parse date_from to get year, month, day
    dt = datetime.strptime(date_from, '%Y-%m-%d')
    year = dt.year
    month = dt.month
    day = dt.day

    #Object key for raw data extraction
    object_key = f'raw/sales/sales_{date_from}.json' 

    # Step 1: Extract raw data from S3
    raw_data = extract_from_s3(object_key)

    if not raw_data:
        logging.info('No raw data to transform. Moving to next record...')
        return

    # Step 2: Transform data
    transformed_df = transformations(raw_data)

    if transformed_df is None or transformed_df.empty:
        logging.info('Transformation resulted in empty DataFrame. Exiting.')
        return
    
    logging.info('Transformation completed.')

    # Step 3: Save transformed data back to S3 in Parquet format
    s3 = S3Client()

    if date_from == date_to:
        target_key = f'curated/sales/year={year}/month={month}/day={day}/sales_{date_from}.parquet'
    else:
        target_key = f'curated/sales/sales_{date_from}_{date_to}.parquet'
    s3.upload_parquet(transformed_df, target_key)
    logging.info(f'Success!✅Transformed data uploaded to s3 with key:{target_key}')
    return     
    
