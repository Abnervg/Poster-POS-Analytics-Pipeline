import logging
import os
import argparse
import dotenv
from datetime import datetime, timedelta
import requests
#=============================
#Import ETL modules
#=============================
from etl.extract import extraction
from etl.transform import transform
from etl.extract_dims import extract_dimensions
from etl.transform_dims import dimension_transform

#=============================
# Configure Logging
#=============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#=============================

# Load environment variables from .env file
env_path = os.getenv("ENV_PATH", "config/.env")
dotenv.load_dotenv(env_path)

def daterange(start_date, end_date):
    """Helper to loop through dates from start to end."""
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def main():
    # Load config 
    config = { "POS_API_TOKEN": os.getenv("POS_API_TOKEN") }
    for key, value in config.items():
        if not value:
            raise ValueError(f"Missing environment variable: {key}")

    logger.info("Starting ETL pipeline...")

    # --- CLI ARGUMENTS ---
    parser = argparse.ArgumentParser(description="Run ETL pipeline steps.")
    
    # 1. Step Control
    parser.add_argument(
        "--step",
        choices=["extract_sales", 'extract_dims', 'transform_sales', 'transform_dims', "all"],
        default="all",
        help="ETL step to run (default: all)",
    )
    
    # 2. Date Control
    parser.add_argument("--start_date", type=str, help="YYYY-MM-DD", required=False)
    parser.add_argument("--end_date", type=str, help="YYYY-MM-DD", required=False)

    args = parser.parse_args()

    # --- DETERMINE DATE RANGE ---
    if args.start_date and args.end_date:
        # Backfill Mode
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        logger.info(f"Mode: BACKFILL ({start_dt} to {end_dt})")
    else:
        # Incremental Mode (Yesterday)
        end_dt = datetime.now().date()
        start_dt = end_dt - timedelta(days=1)
        logger.info(f"Mode: DAILY INCREMENTAL ({start_dt})")

    try:
        # --- DIMENSIONS (Snapshot, so no date loop needed) ---
        if args.step in ("extract_dims", "all"):
            logger.info("--- Step: Extract Dimensions ---")
            extract_dimensions()

        if args.step in ("transform_dims", 'all'):
             logger.info("--- Step: Transform Dimensions ---")
             dimension_transform()

        # --- SALES (Transactional, needs Date Loop) ---
        # We loop through every day to avoid API timeouts and huge file sizes
        with requests.Session() as api_session:
            for single_date in daterange(start_dt, end_dt):
                current_date_str = single_date.strftime('%Y-%m-%d')
                logger.info(f">>> Processing Date: {current_date_str}")

                if args.step in ("extract_sales", "all"):
                    extraction(current_date_str, current_date_str, session=api_session)

                if args.step in ("transform_sales", 'all'):
                    transform(current_date_str, current_date_str)
                
    except Exception as e:
        logger.error(f"ELT pipeline failed: {e}")

if __name__ == "__main__":
    main()