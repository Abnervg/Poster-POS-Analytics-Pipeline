import json
import logging
import os
import argparse
import dotenv
#=============================
#Import ETL modules
#=============================
from etl.extract import extraction
from etl.s3_client import S3Client
from etl.transform import transform
from etl.extract_dims import extract_dimensions

#=============================
# Configure Logging
#=============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#=============================

# Load environment variables from .env file
env_path = os.getenv("ENV_PATH", "config/.env")
dotenv.load_dotenv(env_path)

# Main orchestration function
def main():
    # Load config required variables
    config = {
        "POS_API_TOKEN": os.getenv("POS_API_TOKEN")
    }

    # Make sure all required environment variables are set
    for key, value in config.items():
        if not value:
            raise ValueError(f"Missing environment variable: {key}")

    logger.info("Starting ETL pipeline...")

    parser = argparse.ArgumentParser(description="Run ETL pipeline steps.")
    parser.add_argument(
        "--step",
        choices=["extract_and_load_raw", 'transform', 'extract_dims', 'load_curated' "all"],
        default="all",
        help="ETL step to run (default: all)",
    )
    args = parser.parse_args()

    try:
        if args.step in ("extract_and_load_raw", "all"):
            logger.info("Extracting data...")
            # Extract and Load Raw Data into stage step
            extraction()
            logger.info("Data extraction and raw loading completed.")

        if args.step in ("extract_dims", "all"):
            logger.info("Extracting dimension data...")
            # Extract Dimensions step
            extract_dimensions()
            logger.info("Dimension data loading completed.")

        if args.step in ("transform",'all'):
            logger.info("Transforming data...")
            # Transform step
            transform()
            logger.info("Data transformation completed.")
        
        if args.step in ("load_curated",'all'):
            logger.info("Loading curated data...")
            # Load curated step
            # Call your load curated function here
            logger.info("Curated data loading completed.")


    except Exception as e:
        logger.error(f"ELT pipeline failed: {e}")

if __name__ == "__main__":
    main()