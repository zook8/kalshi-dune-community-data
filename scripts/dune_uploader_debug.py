#!/usr/bin/env python3
"""
DEBUG VERSION - Dune Uploader with Enhanced Logging
Upload Kalshi data to persistent Dune tables with comprehensive debug output
"""

import os
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
from dotenv import load_dotenv
import requests
import json
import time

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

# Load environment variables
load_dotenv(PROJECT_ROOT / "config" / ".env")

# Ensure logs directory exists
(PROJECT_ROOT / "logs").mkdir(exist_ok=True)

# Setup enhanced logging with DEBUG level
log_filename = PROJECT_ROOT / "logs" / f"dune_uploader_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DuneUploaderDebug:
    def __init__(self):
        logger.info("🔧 INITIALIZING DEBUG VERSION OF DUNE UPLOADER")

        self.dune_api_key = os.getenv('DUNE_API_KEY')
        if not self.dune_api_key:
            raise ValueError("DUNE_API_KEY not found in environment variables")

        logger.info(f"🔑 API Key present: {'Yes' if self.dune_api_key else 'No'}")
        logger.info(f"🔑 API Key length: {len(self.dune_api_key) if self.dune_api_key else 0}")

        self.base_url = "https://api.dune.com/api/v1"
        self.headers = {
            'X-DUNE-API-KEY': self.dune_api_key
        }

        self.data_dir = PROJECT_ROOT / "data"
        logger.info(f"📁 Data directory: {self.data_dir}")
        logger.info(f"📁 Data directory exists: {self.data_dir.exists()}")

        # Environment variable debugging
        collection_date_str = os.getenv('COLLECTION_DATE')
        append_mode_str = os.getenv('APPEND_MODE')
        logger.info(f"🌍 COLLECTION_DATE env var: {collection_date_str}")
        logger.info(f"🌍 APPEND_MODE env var: {append_mode_str}")

        if collection_date_str:
            collection_date = datetime.strptime(collection_date_str, '%Y-%m-%d').date()
            self.date_str = collection_date.strftime('%Y%m%d')
            logger.info(f"📅 Using COLLECTION_DATE: {collection_date_str} -> {self.date_str}")
        else:
            self.date_str = datetime.now().strftime('%Y%m%d')
            logger.info(f"📅 Using current date: {self.date_str}")

        # Table names
        self.events_table = "kalshi_events"
        self.markets_table = "kalshi_markets"

        # Append mode
        self.append_mode = os.getenv('APPEND_MODE', 'true').lower() == 'true'
        logger.info(f"🔄 Append mode enabled: {self.append_mode}")

        # Log all environment variables for debugging
        logger.debug("🌍 All environment variables:")
        for key, value in os.environ.items():
            if 'API' in key or 'MODE' in key or 'DATE' in key:
                logger.debug(f"    {key}: {value}")

    def check_data_files(self):
        """Debug: Check if data files exist and their properties"""
        logger.info("🔍 CHECKING DATA FILES EXISTENCE AND PROPERTIES")

        events_file = self.data_dir / f"kalshi_events_{self.date_str}.csv"
        markets_file = self.data_dir / f"kalshi_markets_{self.date_str}.csv"

        # Events file check
        logger.info(f"📄 Events file path: {events_file}")
        logger.info(f"📄 Events file exists: {events_file.exists()}")
        if events_file.exists():
            logger.info(f"📄 Events file size: {events_file.stat().st_size} bytes")
            logger.info(f"📄 Events file modified: {datetime.fromtimestamp(events_file.stat().st_mtime)}")

        # Markets file check
        logger.info(f"📄 Markets file path: {markets_file}")
        logger.info(f"📄 Markets file exists: {markets_file.exists()}")
        if markets_file.exists():
            logger.info(f"📄 Markets file size: {markets_file.stat().st_size} bytes")
            logger.info(f"📄 Markets file modified: {datetime.fromtimestamp(markets_file.stat().st_mtime)}")

            # Quick CSV inspection
            try:
                with open(markets_file, 'r') as f:
                    first_line = f.readline().strip()
                    logger.info(f"📄 Markets CSV header: {first_line[:200]}...")

                    second_line = f.readline().strip()
                    if second_line:
                        logger.info(f"📄 Markets CSV first data row: {second_line[:200]}...")
                    else:
                        logger.warning("📄 Markets CSV has no data rows!")
            except Exception as e:
                logger.error(f"❌ Error reading markets file: {e}")

        # List all files in data directory
        if self.data_dir.exists():
            logger.info("📁 All files in data directory:")
            for file_path in self.data_dir.iterdir():
                logger.info(f"    - {file_path.name} ({file_path.stat().st_size} bytes)")
        else:
            logger.error("❌ Data directory does not exist!")

    def make_dune_request(self, endpoint, method='POST', data=None):
        """Make request to Dune API with enhanced error handling and debugging"""
        try:
            url = f"{self.base_url}{endpoint}"
            logger.debug(f"🌐 Making {method} request to: {url}")

            headers = self.headers.copy()
            headers['Content-Type'] = 'application/json'

            if method == 'POST':
                response = requests.post(url, headers=headers, json=data)
            else:
                response = requests.get(url, headers=headers)

            logger.info(f"📡 Dune API {method} {endpoint}: Status {response.status_code}")
            logger.debug(f"📡 Response headers: {dict(response.headers)}")

            # Handle 409 Conflict as success for table creation
            if response.status_code == 409 and '/table/create' in endpoint:
                logger.info("✅ Table already exists - ready for inserts")
                return {"already_existed": True}

            if response.status_code >= 400:
                logger.error(f"❌ API Error Response: {response.text}")

            response.raise_for_status()
            return response.json() if response.text else {}

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Dune API request failed for {endpoint}: {e}")
            return None

    def get_dune_username(self):
        """Get current Dune username for table naming"""
        return "ghost_in_the_code"

    def create_table_if_not_exists(self, table_name, schema, description):
        """Create Dune table if it doesn't exist with debug output"""
        logger.info(f"🏗️ Creating table if not exists: {table_name}")

        namespace = self.get_dune_username()
        logger.debug(f"🏗️ Using namespace: {namespace}")
        logger.debug(f"🏗️ Schema has {len(schema)} columns")

        payload = {
            "namespace": namespace,
            "table_name": table_name,
            "description": description,
            "is_private": False,
            "schema": schema
        }

        result = self.make_dune_request('/table/create', 'POST', payload)

        if result:
            if result.get('already_existed'):
                logger.info(f"✅ Table {table_name} already exists - ready for inserts")
            else:
                logger.info(f"✅ Created new table: {result.get('full_name')}")
            return True
        else:
            logger.error(f"❌ Failed to create table {table_name}")
            return False

    def clean_data_for_upload(self, df):
        """Clean DataFrame for CSV upload to Dune with debug output"""
        logger.info(f"🧹 Cleaning data for upload - original shape: {df.shape}")

        df_clean = df.copy()

        # Handle numeric columns with detailed logging
        numeric_cols = df_clean.select_dtypes(include=[float, int]).columns
        logger.info(f"🧹 Found {len(numeric_cols)} numeric columns: {list(numeric_cols)}")

        total_cleaned = 0
        for col in numeric_cols:
            # Check for infinite values
            inf_mask = df_clean[col].isin([float('inf'), float('-inf')])
            if inf_mask.any():
                count = inf_mask.sum()
                total_cleaned += count
                logger.warning(f"🧹 Replacing {count} infinite values in column '{col}'")
                df_clean.loc[inf_mask, col] = None

            # Check for very large values
            if df_clean[col].dtype in ['float64', 'int64']:
                large_mask = (df_clean[col].abs() > 1e15) & df_clean[col].notnull()
                if large_mask.any():
                    count = large_mask.sum()
                    total_cleaned += count
                    logger.warning(f"🧹 Replacing {count} extremely large values in column '{col}'")
                    df_clean.loc[large_mask, col] = None

        # Convert NaN/None to empty string
        null_count = df_clean.isnull().sum().sum()
        df_clean = df_clean.fillna('')

        logger.info(f"🧹 Cleaning complete: {total_cleaned} problematic values, {null_count} nulls converted to empty strings")
        logger.info(f"🧹 Final shape: {df_clean.shape}")

        return df_clean

    def insert_data_to_table_direct(self, table_name, df):
        """Insert DataFrame directly to Dune table with comprehensive debugging"""
        logger.info(f"🚀 STARTING INSERT: {len(df)} rows into {table_name}")

        # Comprehensive DataFrame debugging
        logger.info(f"🔍 DataFrame shape: {df.shape}")
        logger.info(f"🔍 DataFrame columns ({len(df.columns)}): {list(df.columns)}")
        logger.info(f"🔍 DataFrame dtypes:\n{df.dtypes}")
        logger.info(f"🔍 DataFrame memory usage: {df.memory_usage(deep=True).sum()} bytes")

        # Show sample data
        logger.info(f"🔍 First 3 rows of DataFrame:")
        for idx, row in df.head(3).iterrows():
            logger.info(f"    Row {idx}: {dict(row)}")

        # Check for empty DataFrame
        if df.empty:
            logger.error(f"🚨 CRITICAL ERROR: DataFrame is EMPTY for {table_name}!")
            return False

        namespace = self.get_dune_username()
        endpoint = f'/table/{namespace}/{table_name}/insert'

        # Clean data
        df_clean = self.clean_data_for_upload(df)

        # CSV conversion with debugging
        csv_buffer = df_clean.to_csv(index=False)
        csv_lines = csv_buffer.split('\n')

        logger.info(f"📄 CSV generation complete:")
        logger.info(f"📄 CSV header: {csv_lines[0]}")
        logger.info(f"📄 CSV first data row: {csv_lines[1] if len(csv_lines) > 1 else 'NO DATA ROWS'}")
        logger.info(f"📄 CSV total lines: {len(csv_lines)}")
        logger.info(f"📄 CSV buffer size: {len(csv_buffer)} characters")

        # Sample a few more rows for debugging
        for i in range(2, min(5, len(csv_lines))):
            if csv_lines[i].strip():
                logger.debug(f"📄 CSV row {i}: {csv_lines[i][:100]}...")

        # API request
        headers = {
            'X-DUNE-API-KEY': self.dune_api_key,
            'Content-Type': 'text/csv'
        }

        try:
            url = f"{self.base_url}{endpoint}"
            logger.info(f"🌐 Upload URL: {url}")
            logger.debug(f"🔑 Request headers: {headers}")

            logger.info(f"📡 Starting HTTP POST request...")
            start_time = time.time()

            response = requests.post(url, headers=headers, data=csv_buffer.encode('utf-8'))

            end_time = time.time()
            logger.info(f"📡 Request completed in {end_time - start_time:.2f} seconds")
            logger.info(f"📡 Response status: {response.status_code}")
            logger.debug(f"📡 Response headers: {dict(response.headers)}")

            if response.text:
                logger.info(f"📡 Response body: {response.text[:1000]}...")

            if response.status_code >= 400:
                logger.error(f"❌ API Error Response: {response.text}")
                return False

            response.raise_for_status()

            logger.info(f"✅ Successfully inserted {len(df)} rows into {namespace}.{table_name}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ HTTP request failed: {e}")
            logger.error(f"❌ Request details:")
            logger.error(f"    URL: {url}")
            logger.error(f"    Data size: {len(csv_buffer)} chars")
            logger.error(f"    Headers: {headers}")
            return False

    def smart_append_data(self, table_name, df_today):
        """Simplified append with debug output"""
        if not self.append_mode:
            logger.info(f"🔄 APPEND_MODE disabled, using clear-and-replace for {table_name}")
            return self.clear_todays_data_via_rebuild(table_name, df_today)

        logger.info(f"🔄 APPEND_MODE enabled for {table_name}")
        logger.info(f"📅 Relying on once-daily workflow schedule to prevent duplicates")
        logger.info(f"📊 Appending {len(df_today)} rows to preserve historical data")

        return self.insert_data_to_table_direct(table_name, df_today)

    def clear_todays_data_via_rebuild(self, table_name, df_today):
        """Clear and rebuild with debug output"""
        logger.info(f"🗑️ Using rebuild approach for {table_name}")

        # Clear table
        namespace = self.get_dune_username()
        endpoint = f'/table/{namespace}/{table_name}/clear'

        result = self.make_dune_request(endpoint, 'POST', {})
        if not result:
            logger.error(f"❌ Failed to clear table {table_name}")
            return False

        logger.info(f"✅ Successfully cleared {table_name}")

        # Insert fresh data
        return self.insert_data_to_table_direct(table_name, df_today)

    def define_markets_schema(self):
        """Define markets schema with debug output"""
        schema = [
            {"name": "ticker", "type": "varchar"},
            {"name": "event_ticker", "type": "varchar"},
            {"name": "market_type", "type": "varchar"},
            {"name": "title", "type": "varchar"},
            {"name": "subtitle", "type": "varchar"},
            {"name": "yes_sub_title", "type": "varchar"},
            {"name": "no_sub_title", "type": "varchar"},
            {"name": "open_time", "type": "varchar"},
            {"name": "close_time", "type": "varchar"},
            {"name": "expected_expiration_time", "type": "varchar"},
            {"name": "expiration_time", "type": "varchar"},
            {"name": "latest_expiration_time", "type": "varchar"},
            {"name": "settlement_timer_seconds", "type": "integer"},
            {"name": "status", "type": "varchar"},
            {"name": "response_price_units", "type": "varchar"},
            {"name": "notional_value", "type": "double"},
            {"name": "notional_value_dollars", "type": "double"},
            {"name": "yes_bid", "type": "double"},
            {"name": "yes_bid_dollars", "type": "double"},
            {"name": "yes_ask", "type": "double"},
            {"name": "yes_ask_dollars", "type": "double"},
            {"name": "no_bid", "type": "double"},
            {"name": "no_bid_dollars", "type": "double"},
            {"name": "no_ask", "type": "double"},
            {"name": "no_ask_dollars", "type": "double"},
            {"name": "last_price", "type": "double"},
            {"name": "last_price_dollars", "type": "double"},
            {"name": "previous_yes_bid", "type": "double"},
            {"name": "previous_yes_bid_dollars", "type": "double"},
            {"name": "previous_yes_ask", "type": "double"},
            {"name": "previous_yes_ask_dollars", "type": "double"},
            {"name": "previous_price", "type": "double"},
            {"name": "previous_price_dollars", "type": "double"},
            {"name": "volume", "type": "integer"},
            {"name": "volume_24h", "type": "integer"},
            {"name": "liquidity", "type": "double"},
            {"name": "liquidity_dollars", "type": "double"},
            {"name": "open_interest", "type": "integer"},
            {"name": "result", "type": "varchar"},
            {"name": "can_close_early", "type": "boolean"},
            {"name": "expiration_value", "type": "varchar"},
            {"name": "category", "type": "varchar"},
            {"name": "risk_limit_cents", "type": "integer"},
            {"name": "strike_type", "type": "varchar"},
            {"name": "custom_strike", "type": "varchar"},
            {"name": "rules_primary", "type": "varchar"},
            {"name": "rules_secondary", "type": "varchar"},
            {"name": "tick_size", "type": "double"},
            {"name": "mve_collection_ticker", "type": "varchar"},
            {"name": "mve_selected_legs", "type": "varchar"},
            {"name": "collection_date", "type": "varchar"},
            {"name": "date", "type": "varchar"},
            {"name": "floor_strike", "type": "double"},
            {"name": "early_close_condition", "type": "varchar"},
            {"name": "cap_strike", "type": "double"},
            {"name": "primary_participant_key", "type": "varchar"},
            {"name": "fee_waiver_expiration_time", "type": "varchar"}
        ]

        logger.debug(f"🏗️ Markets schema defined with {len(schema)} columns")
        return schema

    def test_markets_upload_only(self):
        """Debug function to test ONLY markets upload"""
        logger.info("🧪 TESTING MARKETS UPLOAD ONLY")
        logger.info("=" * 80)

        # Check files first
        self.check_data_files()

        markets_file = self.data_dir / f"kalshi_markets_{self.date_str}.csv"

        if not markets_file.exists():
            logger.error(f"❌ Markets file not found: {markets_file}")
            return False

        try:
            # Load DataFrame
            logger.info(f"📊 Loading markets data from {markets_file}")
            df_markets = pd.read_csv(markets_file, low_memory=False)
            logger.info(f"📊 Successfully loaded {len(df_markets)} markets records")

            if df_markets.empty:
                logger.error(f"🚨 Markets DataFrame is EMPTY!")
                return False

            # Create table
            markets_schema = self.define_markets_schema()
            table_created = self.create_table_if_not_exists(
                self.markets_table,
                markets_schema,
                "Kalshi prediction market individual markets data (DEBUG VERSION)"
            )

            if not table_created:
                logger.error(f"❌ Failed to create markets table")
                return False

            # Process DataFrame
            logger.info(f"🔄 Processing DataFrame...")
            df_markets = df_markets.rename(columns={'DATE': 'date'})

            expected_columns = [col["name"] for col in markets_schema]
            available_columns = [col for col in expected_columns if col in df_markets.columns]
            missing_columns = [col for col in expected_columns if col not in df_markets.columns]

            logger.info(f"🔍 Expected columns: {len(expected_columns)}")
            logger.info(f"🔍 Available columns: {len(available_columns)}")
            if missing_columns:
                logger.warning(f"⚠️ Missing columns: {missing_columns}")

            df_markets = df_markets[available_columns]

            # Upload data
            logger.info(f"🚀 Starting upload of {len(df_markets)} rows...")
            result = self.smart_append_data(self.markets_table, df_markets)

            if result:
                logger.info(f"✅ Markets upload completed successfully!")
            else:
                logger.error(f"❌ Markets upload failed!")

            return result

        except Exception as e:
            logger.error(f"❌ Exception during markets upload: {e}")
            import traceback
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            return False

if __name__ == "__main__":
    try:
        logger.info("🚀 STARTING DEBUG VERSION OF DUNE UPLOADER")
        logger.info(f"📝 Debug log file: {log_filename}")

        uploader = DuneUploaderDebug()

        # Test markets upload only
        success = uploader.test_markets_upload_only()

        if success:
            logger.info("✅ DEBUG TEST COMPLETED SUCCESSFULLY")
            sys.exit(0)
        else:
            logger.error("❌ DEBUG TEST FAILED")
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ Debug script failed: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        sys.exit(1)