#=============================
# ETL - EXTRACT DIMENSIONS MODULE
#=============================

#=============================
#Imports
#=============================
import logging
from dotenv import load_dotenv
from etl.s3_client import S3Client
from datetime import date, timedelta
import os
import requests

#=============================
# Load secret env vars
#=============================
load_dotenv()
#=============================
#Configurations
#=============================
logger = logging.getLogger(__name__)

POSTER_TOKEN = os.getenv('POSTER_TOKEN')
DIMENSIONS_CONFIG = {
    'products': os.getenv('PRODUCTS_URL'),
    'categories': os.getenv('CATEGORIES_URL'),
    'employees': os.getenv('EMPLOYEES_URL'),
    'spots': os.getenv('SPOTS_URL')
}
#=============================
# Functions
#=============================

def fetch_dimension(name,url,session):
    """
    Generic function to fetch dimension data from Poster API
    """
    params = {
        'token': POSTER_TOKEN,
    }

    logger.info(f"Fetching {name} dimension data from Poster API")
    try:
        # Add timeout=30 (seconds)
        # If the server doesn't respond in 30s, it will crash instead of hanging forever
        response = session.get(url, params=params,timeout=30)
        response.raise_for_status()
        data = response.json()

        #Standard Poster response wrapper check
        if 'response' in data:
            return data['response']
        else:
            logger.warning(f"Unexpected response format for {name} dimension")
            return []
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching {name} dimension data: {e}")
        return []
    
def extract_dimensions():
    """
    Loops through dimension configurations and extracts data from Poster API and saves to S3 in json format
    """

    s3 = S3Client()
    today_str = date.today().strftime('%Y-%m-%d')
    session = requests.Session()

    for name, url in DIMENSIONS_CONFIG.items():
        data = fetch_dimension(name, url, session)
        if data:
            s3_key = f"raw/dimensions/{name}/{name}_dim_{today_str}.json"
            s3.upload_json(data, s3_key)
            logger.info(f"Uploaded {name} dimension data to S3 at {s3_key}")
        else:
            logger.warning(f"No data fetched for {name} dimension")

