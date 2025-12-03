#=============================
# ETL: Transform Dimension Data
#=============================
#=============================
# Imports
#=============================
import pandas as pd
import logging
from datetime import date
from etl.s3_client import S3Client
from datetime import timedelta
#=============================
# Logger Configuration
#=============================
logger = logging.getLogger(__name__)
#=============================

# Config: Which columns should be converted to numbers?
NUMERIC_COLS = {
    'products': ['price', 'cost', 'net_cost'],
    'spots': ['profit'],
    'categories': [] # Usually no numeric cols here
}

def transform_single_dimension(name, date_str):
    s3 = S3Client()
    
    # 1. Define Paths
    raw_key = f"raw/dimensions/{name}/{name}_dim_{date_str}.json"
    # OLD
    # target_key = f"curated/dimensions/{name}/{name}_dim_{date_str}.parquet"
    
    # NEW (Hive-Style Partitioning)
    target_key = f"curated/dimensions/{name}/dt={date_str}/{name}.parquet"
    
    # 2. Read Raw Data
    data = s3.read_json(raw_key)
    if not data:
        logger.warning(f"Skipping {name}: No raw data found for {date_str}")
        return

    logger.info(f"Transforming {len(data)} rows for {name}...")

    try:
        # 3. Convert to DataFrame
        df = pd.json_normalize(data)
        
        # 4. Clean Data Types (String -> Number)
        # Only process columns that actually exist in the dataframe
        if name in NUMERIC_COLS:
            for col in NUMERIC_COLS[name]:
                if col in df.columns:
                    # 'coerce' turns bad strings (like "free") into NaN (0)
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 5. Save as Parquet
        s3.upload_parquet(df, target_key)
        logger.info(f"✅ Successfully created {target_key}")

    except Exception as e:
        logger.error(f"❌ Transformation failed for {name}: {e}")

def dimension_transform():
    # We assume we are transforming "Today's" snapshot

    today = date.today()
    logger.info(f"Todays snapshot looks for {today} files...")
    today_str = today.strftime('%Y-%m-%d')
    dimensions = ['products', 'categories', 'spots', 'employees']
    
    for dim in dimensions:
        transform_single_dimension(dim, today_str)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dimension_transform()