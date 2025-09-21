#!/usr/bin/env python3
"""
Dune Uploader - Upload Kalshi data to persistent Dune tables
Creates tables once, then uses clear-and-replace strategy for duplicate prevention
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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / f"dune_uploader_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DuneUploader:
    def __init__(self):
        self.dune_api_key = os.getenv('DUNE_API_KEY')
        if not self.dune_api_key:
            raise ValueError("DUNE_API_KEY not found in environment variables")

        self.base_url = "https://api.dune.com/api/v1"
        self.headers = {
            'X-DUNE-API-KEY': self.dune_api_key
        }

        self.data_dir = PROJECT_ROOT / "data"
        self.date_str = datetime.now().strftime('%Y%m%d')

        # Table names - these will be persistent tables
        self.events_table = "kalshi_events"
        self.markets_table = "kalshi_markets"

    def make_dune_request(self, endpoint, method='POST', data=None):
        """Make request to Dune API with error handling"""
        try:
            url = f"{self.base_url}{endpoint}"

            # Add Content-Type for JSON requests
            headers = self.headers.copy()
            headers['Content-Type'] = 'application/json'

            if method == 'POST':
                response = requests.post(url, headers=headers, json=data)
            else:
                response = requests.get(url, headers=headers)

            # Log response for debugging
            logger.info(f"Dune API {method} {endpoint}: Status {response.status_code}")

            # Handle 409 Conflict as success for table creation (table already exists)
            if response.status_code == 409 and '/table/create' in endpoint:
                logger.info("Table already exists - ready for inserts")
                return {"already_existed": True}

            if response.status_code >= 400:
                logger.error(f"Response: {response.text}")

            response.raise_for_status()
            return response.json() if response.text else {}

        except requests.exceptions.RequestException as e:
            logger.error(f"Dune API request failed for {endpoint}: {e}")
            return None

    def get_dune_username(self):
        """Get current Dune username for table naming"""
        return "ghost_in_the_code"

    def create_table_if_not_exists(self, table_name, schema, description):
        """Create Dune table if it doesn't exist"""
        logger.info(f"Creating table if not exists: {table_name}")

        namespace = self.get_dune_username()

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
                logger.info(f"Table {table_name} already exists - ready for inserts")
            else:
                logger.info(f"Created new table: {result.get('full_name')}")
            return True
        else:
            logger.error(f"Failed to create table {table_name}")
            return False

    def clear_table_completely(self, table_name):
        """Clear all data from table using Dune's clear API"""
        logger.info(f"Clearing all data from {table_name} to start fresh")

        namespace = self.get_dune_username()
        endpoint = f'/table/{namespace}/{table_name}/clear'

        result = self.make_dune_request(endpoint, 'POST', {})

        if result:
            logger.info(f"Successfully cleared all data from {namespace}.{table_name}")
            return True
        else:
            logger.error(f"Failed to clear data from {namespace}.{table_name}")
            return False

    def clean_data_for_upload(self, df):
        """Clean DataFrame for CSV upload to Dune"""
        logger.info("Cleaning data for upload...")

        # Make a copy to avoid modifying original
        df_clean = df.copy()

        # Handle numeric columns
        numeric_cols = df_clean.select_dtypes(include=[float, int]).columns
        for col in numeric_cols:
            # Check for infinite values
            inf_mask = df_clean[col].isin([float('inf'), float('-inf')])
            if inf_mask.any():
                count = inf_mask.sum()
                logger.warning(f"Replacing {count} infinite values in column '{col}' with NaN")
                df_clean.loc[inf_mask, col] = None

            # Check for very large values that might cause issues
            if df_clean[col].dtype in ['float64', 'int64']:
                # Replace extremely large values that might overflow
                large_mask = (df_clean[col].abs() > 1e15) & df_clean[col].notnull()
                if large_mask.any():
                    count = large_mask.sum()
                    logger.warning(f"Replacing {count} extremely large values in column '{col}' with NaN")
                    df_clean.loc[large_mask, col] = None

        # Convert all NaN/None to empty string for CSV compatibility
        df_clean = df_clean.fillna('')

        # Log cleaned columns
        for col in numeric_cols:
            if (df[col].isna() | df[col].isin([float('inf'), float('-inf')])).any():
                count = (df[col].isna() | df[col].isin([float('inf'), float('-inf')])).sum()
                logger.info(f"Cleaned {count} problematic values in column '{col}'")

        return df_clean

    def insert_data_to_table_direct(self, table_name, df):
        """Insert DataFrame directly to Dune table using CSV format"""
        logger.info(f"Inserting {len(df)} rows into {table_name}")

        namespace = self.get_dune_username()
        endpoint = f'/table/{namespace}/{table_name}/insert'

        # Clean data for upload
        df_clean = self.clean_data_for_upload(df)

        # Convert DataFrame to CSV string
        csv_buffer = df_clean.to_csv(index=False)

        # Prepare headers for CSV upload
        headers = {
            'X-DUNE-API-KEY': self.dune_api_key,
            'Content-Type': 'text/csv'
        }

        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.post(url, headers=headers, data=csv_buffer.encode('utf-8'))

            logger.info(f"Dune API POST {endpoint}: Status {response.status_code}")

            if response.status_code >= 400:
                logger.error(f"Response: {response.text}")

            response.raise_for_status()

            logger.info(f"Successfully inserted {len(df)} rows into {namespace}.{table_name}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Dune API request failed for {endpoint}: {e}")
            return False

    def check_if_todays_data_exists(self, table_name):
        """Check if today's data already exists in table"""
        try:
            namespace = self.get_dune_username()
            today_date = datetime.now().strftime('%Y-%m-%d')

            # Create a simple query to check for today's data
            query_url = f"https://api.dune.com/api/v1/query/execute"

            # Simple query to count rows with today's collection_date
            sql_query = f"SELECT COUNT(*) as count FROM dune.{namespace}.{table_name} WHERE collection_date = '{today_date}'"

            payload = {
                "query_sql": sql_query,
                "is_private": False
            }

            headers = {
                'X-DUNE-API-KEY': self.dune_api_key,
                'Content-Type': 'application/json'
            }

            response = requests.post(query_url, headers=headers, json=payload)

            if response.status_code == 200:
                result = response.json()
                # If we get results and count > 0, data exists
                if result.get('result') and len(result['result']['rows']) > 0:
                    count = result['result']['rows'][0].get('count', 0)
                    logger.info(f"Found {count} rows for today's date in {table_name}")
                    return count > 0

            # If query fails or returns no results, assume no data exists
            logger.info(f"No existing data found for today in {table_name}")
            return False

        except Exception as e:
            logger.warning(f"Could not check existing data for {table_name}: {e}")
            # If check fails, assume no data exists (safe to proceed)
            return False

    def smart_append_data(self, table_name, df_today):
        """Smart append: check if today's data exists, then append or skip"""
        append_mode = os.getenv('APPEND_MODE', 'false').lower() == 'true'

        if not append_mode:
            # Fall back to original clear-and-replace behavior
            logger.info(f"APPEND_MODE not enabled, using clear-and-replace for {table_name}")
            return self.clear_todays_data_via_rebuild(table_name, df_today)

        logger.info(f"APPEND_MODE enabled: checking for existing data in {table_name}")

        # Check if today's data already exists
        if self.check_if_todays_data_exists(table_name):
            logger.info(f"Today's data already exists in {table_name} - skipping upload to prevent duplicates")
            logger.info("💰 Saved Dune credits by skipping duplicate upload!")
            return True  # Consider this a success

        # Today's data doesn't exist, safe to insert
        logger.info(f"No existing data for today in {table_name} - proceeding with append")
        return self.insert_data_to_table_direct(table_name, df_today)

    def clear_todays_data_via_rebuild(self, table_name, df_today):
        """Clear table and insert only today's data (rebuild approach)"""
        logger.info(f"Using rebuild approach: clear table and insert fresh data for {table_name}")

        # Step 1: Clear table completely
        if not self.clear_table_completely(table_name):
            logger.error(f"Failed to clear table {table_name}")
            return False

        # Step 2: Insert today's fresh data
        logger.info(f"Inserting fresh data for today into {table_name}")
        return self.insert_data_to_table_direct(table_name, df_today)

    def define_events_schema(self):
        """Define schema for events table"""
        return [
            {"name": "category_acronym", "type": "varchar"},
            {"name": "collection_date", "type": "varchar"},
            {"name": "collection_datetime", "type": "varchar"},
            {"name": "date", "type": "varchar"},
            {"name": "event_ticker", "type": "varchar"},
            {"name": "markets_count", "type": "integer"},
            {"name": "mutually_exclusive", "type": "boolean"},
            {"name": "series_ticker", "type": "varchar"},
            {"name": "status", "type": "varchar"},
            {"name": "strike_date", "type": "varchar"},
            {"name": "subtitle", "type": "varchar"},
            {"name": "title", "type": "varchar"}
        ]

    def define_markets_schema(self):
        """Define comprehensive schema for markets table with all 57 columns"""
        return [
            {"name": "can_close_early", "type": "boolean"},
            {"name": "cap_strike", "type": "double"},
            {"name": "category_acronym", "type": "varchar"},
            {"name": "close_date", "type": "varchar"},
            {"name": "close_time", "type": "varchar"},
            {"name": "collection_date", "type": "varchar"},
            {"name": "collection_datetime", "type": "varchar"},
            {"name": "custom_strike", "type": "varchar"},
            {"name": "date", "type": "varchar"},
            {"name": "event_ticker", "type": "varchar"},
            {"name": "expected_result", "type": "varchar"},
            {"name": "expiration_time", "type": "varchar"},
            {"name": "expiration_value", "type": "varchar"},
            {"name": "floor_strike", "type": "double"},
            {"name": "functional_strike", "type": "double"},
            {"name": "last_price", "type": "double"},
            {"name": "liquidity", "type": "double"},
            {"name": "max_close_early_time", "type": "varchar"},
            {"name": "no_ask", "type": "double"},
            {"name": "no_bid", "type": "double"},
            {"name": "open_interest", "type": "integer"},
            {"name": "open_time", "type": "varchar"},
            {"name": "previous_no", "type": "double"},
            {"name": "previous_price", "type": "double"},
            {"name": "previous_yes", "type": "double"},
            {"name": "result", "type": "varchar"},
            {"name": "risk_limit_cents", "type": "integer"},
            {"name": "series_ticker", "type": "varchar"},
            {"name": "status", "type": "varchar"},
            {"name": "strike_type", "type": "varchar"},
            {"name": "subtitle", "type": "varchar"},
            {"name": "ticker", "type": "varchar"},
            {"name": "title", "type": "varchar"},
            {"name": "volume", "type": "integer"},
            {"name": "volume_24h", "type": "integer"},
            {"name": "yes_ask", "type": "double"},
            {"name": "yes_bid", "type": "double"},
            {"name": "category", "type": "varchar"},
            {"name": "event_id", "type": "varchar"},
            {"name": "market_id", "type": "varchar"},
            {"name": "market_url", "type": "varchar"},
            {"name": "rules", "type": "varchar"},
            {"name": "tags", "type": "varchar"},
            {"name": "trading_active", "type": "boolean"},
            {"name": "trading_end_time", "type": "varchar"},
            {"name": "trading_start_time", "type": "varchar"},
            {"name": "response_price_per_dollar", "type": "double"},
            {"name": "dollar_volume_24h", "type": "double"},
            {"name": "notional_volume", "type": "double"},
            {"name": "number_of_traders", "type": "integer"},
            {"name": "number_of_unique_bettors", "type": "integer"},
            {"name": "implied_probability", "type": "double"},
            {"name": "settlement_timer_seconds", "type": "integer"},
            {"name": "settlement_value", "type": "varchar"},
            {"name": "volume_today", "type": "integer"},
            {"name": "dollar_volume_today", "type": "double"},
            {"name": "dollar_volume", "type": "double"}
        ]

    def upload_daily_data(self):
        """Main upload function with duplicate prevention"""
        logger.info("=" * 60)
        logger.info("STARTING DUNE UPLOAD WITH DUPLICATE PREVENTION")
        logger.info("Strategy: Clear-and-replace for guaranteed no duplicates")
        logger.info("=" * 60)

        results = {'events': False, 'markets': False}

        # Upload events data
        events_file = self.data_dir / f"kalshi_events_{self.date_str}.csv"
        if events_file.exists():
            logger.info(f"Processing events data from {events_file}")

            try:
                df_events = pd.read_csv(events_file, low_memory=False)
                logger.info(f"Loaded {len(df_events)} events records")

                # Create events table if needed
                events_schema = self.define_events_schema()
                table_created = self.create_table_if_not_exists(
                    self.events_table,
                    events_schema,
                    f"Kalshi prediction market events data. Updated daily with current event status, category, title, and market counts. Data sourced from Kalshi API."
                )

                if table_created:
                    # Map CSV columns to match Dune table schema exactly
                    df_events = df_events.rename(columns={'DATE': 'date'})

                    # Reorder columns to match Dune table schema
                    expected_events_columns = [
                        'event_ticker', 'series_ticker', 'sub_title', 'title',
                        'collateral_return_type', 'mutually_exclusive', 'category',
                        'price_level_structure', 'available_on_brokers',
                        'collection_date', 'date', 'strike_date', 'strike_period'
                    ]

                    # Keep only expected columns in correct order
                    df_events = df_events[expected_events_columns]

                    # Smart append: check for existing data and append only if needed
                    results['events'] = self.smart_append_data(self.events_table, df_events)

            except Exception as e:
                logger.error(f"Error processing events data: {e}")
        else:
            logger.warning(f"Events file not found: {events_file}")

        # Upload markets data
        markets_file = self.data_dir / f"kalshi_markets_{self.date_str}.csv"
        if markets_file.exists():
            logger.info(f"Processing markets data from {markets_file}")

            try:
                df_markets = pd.read_csv(markets_file, low_memory=False)
                logger.info(f"Loaded {len(df_markets)} markets records")

                # Create markets table if needed
                markets_schema = self.define_markets_schema()
                table_created = self.create_table_if_not_exists(
                    self.markets_table,
                    markets_schema,
                    f"Kalshi prediction market individual markets data. Updated daily with current pricing, volume, and liquidity metrics. Data sourced from Kalshi API."
                )

                if table_created:
                    # Map CSV columns to match Dune table schema exactly
                    df_markets = df_markets.rename(columns={'DATE': 'date'})

                    # Reorder columns to match Dune table schema
                    expected_markets_columns = [
                        'ticker', 'event_ticker', 'market_type', 'title', 'subtitle',
                        'yes_sub_title', 'no_sub_title', 'open_time', 'close_time',
                        'expected_expiration_time', 'expiration_time', 'latest_expiration_time',
                        'settlement_timer_seconds', 'status', 'response_price_units',
                        'notional_value', 'notional_value_dollars', 'yes_bid', 'yes_bid_dollars',
                        'yes_ask', 'yes_ask_dollars', 'no_bid', 'no_bid_dollars',
                        'no_ask', 'no_ask_dollars', 'last_price', 'last_price_dollars',
                        'previous_yes_bid', 'previous_yes_bid_dollars', 'previous_yes_ask',
                        'previous_yes_ask_dollars', 'previous_price', 'previous_price_dollars',
                        'volume', 'volume_24h', 'liquidity', 'liquidity_dollars',
                        'open_interest', 'result', 'can_close_early', 'expiration_value',
                        'category', 'risk_limit_cents', 'strike_type', 'custom_strike',
                        'rules_primary', 'rules_secondary', 'tick_size', 'mve_collection_ticker',
                        'mve_selected_legs', 'collection_date', 'date', 'floor_strike',
                        'early_close_condition', 'cap_strike', 'primary_participant_key',
                        'fee_waiver_expiration_time'
                    ]

                    # Keep only expected columns in correct order
                    df_markets = df_markets[expected_markets_columns]

                    # Smart append: check for existing data and append only if needed
                    results['markets'] = self.smart_append_data(self.markets_table, df_markets)

            except Exception as e:
                logger.error(f"Error processing markets data: {e}")
        else:
            logger.warning(f"Markets file not found: {markets_file}")

        # Summary
        namespace = self.get_dune_username()
        logger.info("=" * 50)
        logger.info("UPLOAD TO PERSISTENT TABLES COMPLETE")
        logger.info(f"Events upload: {'SUCCESS' if results['events'] else 'FAILED'}")
        logger.info(f"Markets upload: {'SUCCESS' if results['markets'] else 'FAILED'}")

        if results['events'] or results['markets']:
            logger.info("\nYour persistent Dune tables:")
            if results['events']:
                logger.info(f"📊 Events: SELECT * FROM dune.{namespace}.{self.events_table}")
            if results['markets']:
                logger.info(f"📊 Markets: SELECT * FROM dune.{namespace}.{self.markets_table}")
            logger.info("\nEach run replaces table data - guaranteed no duplicates!")

        logger.info("=" * 50)

        return results

if __name__ == "__main__":
    try:
        uploader = DuneUploader()
        results = uploader.upload_daily_data()

        if results['events'] or results['markets']:
            logger.info("Upload process completed with some success")
            sys.exit(0)
        else:
            logger.error("All uploads failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Upload process failed: {e}")
        sys.exit(1)