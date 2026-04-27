import requests
import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import schedule
import time
import logging
from dotenv import load_dotenv
import os
import json 

load_dotenv()

# -------------------------------
# LOGGING SETUP
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# -------------------------------
# CONFIGURATION
# -------------------------------
API_KEY = os.getenv('TWELVE_DATA_API_KEY', 'demo')
SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN']

# PostgreSQL connection using SQLAlchemy
DB_CONFIG = {
    'dbname': 'stock_data_warehouse',
    'user': 'postgres',
    'password': os.getenv('DB_PASSWORD'),
    'host': 'localhost',
    'port': '5432'
}

# Create SQLAlchemy engine
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# -------------------------------
# E X T R A C T
# -------------------------------
def extract_data(symbol):
    """Extract raw stock data from Twelve Data API using time_series endpoint"""
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval=1day&outputsize=1&apikey={API_KEY}"

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        # Check for API errors
        if 'code' in data:
            if data['code'] == 429:
                logging.warning(f"Rate limit hit for {symbol}")
            else:
                logging.warning(f"API error for {symbol}: {data.get('message', 'Unknown error')}")
            return None

        # Check if we have values
        if 'values' not in data or not data['values']:
            logging.warning(f"No time series data for {symbol}, trying fallback...")
            return extract_price_fallback(symbol)

        # Get the most recent value
        latest = data['values'][0]
        result = {
            'symbol': symbol,
            'price': float(latest.get('close', 0)),
            'volume': int(latest.get('volume', 0)),
            'open': float(latest.get('open', 0)),
            'high': float(latest.get('high', 0)),
            'low': float(latest.get('low', 0)),
        }

        logging.info(f"Extracted data for {symbol}: ${result['price']}")
        return result

    except Exception as e:
        logging.error(f"Extract failed for {symbol}: {e}")
        return None

def extract_price_fallback(symbol):
    """Fallback to /price endpoint if time_series fails"""
    url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={API_KEY}"

    try:
        response = requests.get(url, timeout=15)
        data = response.json()

        if 'price' in data:
            price = float(data['price'])
            logging.info(f"Fallback: Got price for {symbol}: ${price}")
            return {
                'symbol': symbol,
                'price': price,
                'volume': 0,
                'open': price,
                'high': price,
                'low': price,
            }
        else:
            error_msg = data.get('message', 'No price data')
            logging.warning(f"Fallback failed for {symbol}: {error_msg}")
            return None
    except Exception as e:
        logging.error(f"Fallback error for {symbol}: {e}")
        return None

# -------------------------------
# T R A N S F O R M
# -------------------------------
def transform_data(raw_data, symbol):
    """Clean and structure raw API data"""
    try:
        transformed = {
            'symbol': symbol,
            'price': float(raw_data.get('price', 0)),
            'volume': int(raw_data.get('volume', 0)),
            'open_price': float(raw_data.get('open', 0)),
            'high_price': float(raw_data.get('high', 0)),
            'low_price': float(raw_data.get('low', 0)),
            'trade_date': datetime.now().date(),
            'etl_timestamp': datetime.now(),
            # FIX HERE: Convert dict to proper JSON string
            'raw_json': json.dumps(raw_data)  # Changed from str() to json.dumps()
        }

        # Data validation
        if transformed['price'] <= 0:
            logging.warning(f"Invalid price {transformed['price']} for {symbol}")
            return None

        logging.info(f"Transformed data for {symbol}: ${transformed['price']}")
        return transformed

    except Exception as e:
        logging.error(f"Transform failed for {symbol}: {e}")
        return None

# -------------------------------
# L O A D
# -------------------------------
def load_to_postgres(transformed_data):
    """Load transformed data into PostgreSQL using SQLAlchemy"""
    if not transformed_data:
        return False

    try:
        with engine.connect() as conn:
            # Insert into staging table
            staging_query = text("""
                INSERT INTO staging_stock_data (symbol, raw_json, ingested_at)
                VALUES (:symbol, CAST(:raw_json AS JSONB), :ingested_at)
            """)
            conn.execute(staging_query, {
                'symbol': transformed_data['symbol'],
                'raw_json': transformed_data['raw_json'],
                'ingested_at': transformed_data['etl_timestamp']
            })

            # Insert into warehouse table
            warehouse_query = text("""
                INSERT INTO warehouse_stock_prices 
                (symbol, price, volume, open_price, high_price, low_price, trade_date, etl_timestamp)
                VALUES (:symbol, :price, :volume, :open_price, :high_price, :low_price, :trade_date, :etl_timestamp)
                ON CONFLICT (symbol, trade_date) 
                DO UPDATE SET 
                    price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    etl_timestamp = EXCLUDED.etl_timestamp
            """)
            conn.execute(warehouse_query, {
                'symbol': transformed_data['symbol'],
                'price': transformed_data['price'],
                'volume': transformed_data['volume'],
                'open_price': transformed_data['open_price'],
                'high_price': transformed_data['high_price'],
                'low_price': transformed_data['low_price'],
                'trade_date': transformed_data['trade_date'],
                'etl_timestamp': transformed_data['etl_timestamp']
            })

            conn.commit()
            logging.info(f"Loaded {transformed_data['symbol']} into PostgreSQL")
            return True

    except Exception as e:
        logging.error(f"Load failed: {e}")
        return False

# -------------------------------
# E T L   P I P E L I N E
# -------------------------------
def run_etl_pipeline():
    """Main ETL orchestration function"""
    run_start = datetime.now()
    logging.info(f"🚀 ETL Pipeline STARTED at {run_start}")

    # Log pipeline run
    with engine.connect() as conn:
        result = conn.execute(
            text("INSERT INTO etl_metadata (start_time, status) VALUES (:start_time, 'RUNNING') RETURNING run_id"),
            {'start_time': run_start}
        )
        run_id = result.fetchone()[0]
        conn.commit()

    records_extracted = 0
    records_loaded = 0

    for symbol in SYMBOLS:
        # EXTRACT
        raw_data = extract_data(symbol)
        if raw_data:
            records_extracted += 1

            # TRANSFORM
            clean_data = transform_data(raw_data, symbol)

            # LOAD
            if clean_data and load_to_postgres(clean_data):
                records_loaded += 1

        time.sleep(2)  # Respect rate limits

    # Update metadata
    with engine.connect() as conn:
        conn.execute(
            text("""
                UPDATE etl_metadata 
                SET end_time = :end_time, records_extracted = :extracted, records_loaded = :loaded, status = 'COMPLETED'
                WHERE run_id = :run_id
            """),
            {
                'end_time': datetime.now(),
                'extracted': records_extracted,
                'loaded': records_loaded,
                'run_id': run_id
            }
        )
        conn.commit()

    logging.info(f"✅ ETL Pipeline COMPLETED - Extracted: {records_extracted}, Loaded: {records_loaded}")

# -------------------------------
# M A I N
# -------------------------------
if __name__ == "__main__":
    logging.info("📊 Stock Market ETL Pipeline Started")
    logging.info(f"Tracking: {', '.join(SYMBOLS)}")
    logging.info(f"Using API Key: {API_KEY[:10]}..." if API_KEY != 'demo' else "Using demo API key")
    logging.info("Database: stock_data_warehouse (manage via pgAdmin4)")

    # Test API connection first
    logging.info("Testing API connection...")
    test_symbol = "AAPL"
    test_data = extract_data(test_symbol)
    if test_data:
        logging.info(f"✅ API test successful! Got {test_symbol} price: ${test_data['price']}")
    else:
        logging.warning(f"⚠️ API test failed. Please check your API key or network connection.")

    # Run immediately
    run_etl_pipeline()

    # Schedule every 5 minutes
    schedule.every(5).minutes.do(run_etl_pipeline)

    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(1)



