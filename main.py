import json
import logging
import os
from etl.extract import extraction
import argparse
import dotenv

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

    parser = argparse.ArgumentParser(description="Run EL pipeline steps.")
    parser.add_argument(
        "--step",
        choices=["extract_and_load", "all"],
        default="all",
        help="ELT step to run (default: all)",
    )
    args = parser.parse_args()

    try:
        if args.step in ("extract_and_load", "all"):
            logger.info("Extracting data...")
            # Extract and Load step
            extraction()
            logger.info("Data extraction and loading completed.")
        logger.info("EL pipeline finished successfully.")

    except Exception as e:
        logger.error(f"ELT pipeline failed: {e}")

if __name__ == "__main__":
    main()