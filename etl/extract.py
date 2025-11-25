import requests
import os
import json
import logging
from datetime import date, timedelta
from dotenv import load_dotenv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from etl.load import save_raw_to_s3
from pathlib import Path

#Load secret env vars
load_dotenv()

#=============================
#Configurations
#=============================
POSTER_TOKEN = os.getenv('POSTER_TOKEN')
S3_BUCKET = os.getenv('S3_BUCKET')
TRANSACTION_URL = os.getenv('TRANSACTION_URL')
RECEIPT_URL = os.getenv('RECEIPT_URL')


#Local path for debugging purposes
#DATA_RAW_PATH = Path('data/raw/')

#logging configuration
logger = logging.getLogger(__name__)

#Limit parallel requests to 5 to avoid overwhelming the API
MAX_PARALLEL_REQUESTS = 5


#=============================
# Fetch transaction from Poster API within a date range
# =============================
# Args:
#    date_from (str): The start date in 'YYYYMMDD' format.
#    date_to (str): The end date in 'YYYYMMDD' format.
# Returns:
#  list: A list of transaction.
# =============================

#Step 1: Fetch list of transaction headers to get the 'transaction_ids' for every sale.
def get_transactions(date_from,date_to):

    """
    Fetches transaction IDs from the Poster API for a specific date range.
    It performs the requests in parallel to improve efficiency.
    """

    url = (
        f'{TRANSACTION_URL}'
    )
    params = {
        'token': POSTER_TOKEN,
        'date_from': date_from,
        'date_to': date_to,
        'include_receipts': 'true',
        'include_delivery': 'true'
    }

    logging.info('Fetching transactions from Poster API...')

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Check for HTTP errors (like 4xx or 5xx)
        data = response.json()

        if 'response' in data and isinstance(data['response'], list):
            transaction_headers = data['response']
            transaction_ids = [header['transaction_id'] for header in transaction_headers]
            logging.info(f'Fetched {len(transaction_ids)} transaction IDs.')
            return data
        else:
            logging.warning('No transaction headers found in the response.')
            return []

    except requests.exceptions.RequestException as e:
        logging.error(f'An error occurred while fetching transaction IDs: {e}')
        return []
    

#=============================
# Main function to orchestrate 
# fetching transactions and 
# their details
#=============================

def extraction():
    # Example date range: last 3 days
    date_to = date.today() - timedelta(days=1)
    date_from = date_to - timedelta(days=2)
    date_from_str = date_from.strftime('%Y%m%d')
    date_to_str = date_to.strftime('%Y%m%d')

    # Step 1: Fetch transaction IDs
    transactions = get_transactions(date_from_str, date_to_str)

    if not transactions:
        logging.info('No transaction IDs to process. Exiting.')
        return

    #Save receipts data locally (optional)
    #output_dir = DATA_RAW_PATH
    #output_dir.mkdir(parents=True, exist_ok=True)
    #output_file = output_dir / f'receipts_from_{date_from_str}_to_{date_to_str}.json'
    #with open(output_file, 'w') as f:
    #    json.dump(receipts, f, indent=4)

    # Load receipts to S3
    save_raw_to_s3(transactions, date_from_str, date_to_str) 

    logging.info(f'Receipt details saved to s3 raw data.')



