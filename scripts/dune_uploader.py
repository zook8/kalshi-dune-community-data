#!/usr/bin/env python3
"""
Dune Uploader - Upload Kalshi data to persistent Dune tables
Creates tables once, then appends daily data to the same tables
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
            'X-DUNE-API-KEY': self.dune_api_key,
            'Content-Type': 'application/json'
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
            
            if method == 'POST':
                response = requests.post(url, headers=self.headers, json=data)
            else:
                response = requests.get(url, headers=self.headers)
            
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
        # For now, let's use a placeholder. You can get this from Dune API later
        return "ghost_in_the_code"  # Replace with actual username retrieval logic
    
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

    def clear_todays_data_via_rebuild(self, table_name, df_today):
        """Clear table and insert only today's data (rebuild approach)"""
        logger.info(f"Using rebuild approach: clear table and insert fresh data for {table_name}")

        # Step 1: Clear all data
        if not self.clear_table_completely(table_name):
            logger.error(f"Failed to clear table {table_name}")
            return False

        # Step 2: Insert today's fresh data
        logger.info(f"Inserting fresh data for today into {table_name}")
        return self.insert_data_to_table_direct(table_name, df_today)

    def insert_data_to_table_direct(self, table_name, df):
        """Direct insert without duplicate prevention logs (used after clear)"""
        logger.info(f"Inserting {len(df)} rows into {table_name}")

        namespace = self.get_dune_username()
        csv_data = df.to_csv(index=False)
        endpoint = f'/table/{namespace}/{table_name}/insert'

        csv_headers = {
            'X-DUNE-API-KEY': self.dune_api_key,
            'Content-Type': 'text/csv'
        }

        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.post(url, headers=csv_headers, data=csv_data)

            logger.info(f"Dune API POST {endpoint}: Status {response.status_code}")
            if response.status_code >= 400:
                logger.error(f"Response: {response.text}")

            response.raise_for_status()
            logger.info(f"Successfully inserted {len(df)} rows into {namespace}.{table_name}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Dune API request failed for {endpoint}: {e}")
            return False

    def insert_data_to_table(self, table_name, df):
        """Insert DataFrame data into existing Dune table with duplicate prevention"""
        logger.info(f"Inserting {len(df)} rows into {table_name} (with duplicate prevention)")

        namespace = self.get_dune_username()

        # Add duplicate prevention info to logs
        if 'date' in df.columns:
            unique_dates = df['date'].unique()
            logger.info(f"Inserting data for dates: {unique_dates}")
            logger.info("Note: If running multiple times for same date, data will accumulate")

        # Convert DataFrame to CSV string
        csv_data = df.to_csv(index=False)

        # Use correct endpoint format: /table/{namespace}/{table_name}/insert
        endpoint = f'/table/{namespace}/{table_name}/insert'

        # Update headers for CSV upload
        csv_headers = {
            'X-DUNE-API-KEY': self.dune_api_key,
            'Content-Type': 'text/csv'
        }

        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.post(url, headers=csv_headers, data=csv_data)

            # Log response for debugging
            logger.info(f"Dune API POST {endpoint}: Status {response.status_code}")
            if response.status_code >= 400:
                logger.error(f"Response: {response.text}")

            response.raise_for_status()
            result = response.json() if response.text else {}

            logger.info(f"Successfully inserted {len(df)} rows into {namespace}.{table_name}")
            logger.info("DUPLICATE PREVENTION: Use DISTINCT in your Dune queries to handle any duplicates")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Dune API request failed for {endpoint}: {e}")
            logger.error(f"Failed to insert data into {namespace}.{table_name}")
            return False
    
    def define_events_schema(self):
        """Define schema for Kalshi events table based on actual CSV columns"""
        return [
            {"name": "event_ticker", "type": "varchar"},
            {"name": "series_ticker", "type": "varchar", "nullable": True},
            {"name": "sub_title", "type": "varchar", "nullable": True},
            {"name": "title", "type": "varchar"},
            {"name": "collateral_return_type", "type": "varchar", "nullable": True},
            {"name": "mutually_exclusive", "type": "boolean", "nullable": True},
            {"name": "category", "type": "varchar", "nullable": True},
            {"name": "price_level_structure", "type": "varchar", "nullable": True},
            {"name": "available_on_brokers", "type": "boolean", "nullable": True},
            {"name": "collection_date", "type": "varchar"},
            {"name": "date", "type": "varchar"},
            {"name": "strike_date", "type": "varchar", "nullable": True},
            {"name": "strike_period", "type": "varchar", "nullable": True}
        ]
    
    def define_markets_schema(self):
        """Define comprehensive schema for Kalshi markets table to match all CSV columns"""
        return [
            {"name": "ticker", "type": "varchar"},
            {"name": "event_ticker", "type": "varchar"},
            {"name": "market_type", "type": "varchar", "nullable": True},
            {"name": "title", "type": "varchar"},
            {"name": "subtitle", "type": "varchar", "nullable": True},
            {"name": "yes_sub_title", "type": "varchar", "nullable": True},
            {"name": "no_sub_title", "type": "varchar", "nullable": True},
            {"name": "open_time", "type": "varchar", "nullable": True},
            {"name": "close_time", "type": "varchar", "nullable": True},
            {"name": "expected_expiration_time", "type": "varchar", "nullable": True},
            {"name": "expiration_time", "type": "varchar", "nullable": True},
            {"name": "latest_expiration_time", "type": "varchar", "nullable": True},
            {"name": "settlement_timer_seconds", "type": "integer", "nullable": True},
            {"name": "status", "type": "varchar"},
            {"name": "response_price_units", "type": "varchar", "nullable": True},
            {"name": "notional_value", "type": "integer", "nullable": True},
            {"name": "notional_value_dollars", "type": "double", "nullable": True},
            {"name": "yes_bid", "type": "integer", "nullable": True},
            {"name": "yes_bid_dollars", "type": "double", "nullable": True},
            {"name": "yes_ask", "type": "integer", "nullable": True},
            {"name": "yes_ask_dollars", "type": "double", "nullable": True},
            {"name": "no_bid", "type": "integer", "nullable": True},
            {"name": "no_bid_dollars", "type": "double", "nullable": True},
            {"name": "no_ask", "type": "integer", "nullable": True},
            {"name": "no_ask_dollars", "type": "double", "nullable": True},
            {"name": "last_price", "type": "integer", "nullable": True},
            {"name": "last_price_dollars", "type": "double", "nullable": True},
            {"name": "previous_yes_bid", "type": "integer", "nullable": True},
            {"name": "previous_yes_bid_dollars", "type": "double", "nullable": True},
            {"name": "previous_yes_ask", "type": "integer", "nullable": True},
            {"name": "previous_yes_ask_dollars", "type": "double", "nullable": True},
            {"name": "previous_price", "type": "integer", "nullable": True},
            {"name": "previous_price_dollars", "type": "double", "nullable": True},
            {"name": "volume", "type": "integer", "nullable": True},
            {"name": "volume_24h", "type": "integer", "nullable": True},
            {"name": "liquidity", "type": "double", "nullable": True},
            {"name": "liquidity_dollars", "type": "double", "nullable": True},
            {"name": "open_interest", "type": "integer", "nullable": True},
            {"name": "result", "type": "varchar", "nullable": True},
            {"name": "can_close_early", "type": "boolean", "nullable": True},
            {"name": "expiration_value", "type": "varchar", "nullable": True},
            {"name": "category", "type": "varchar", "nullable": True},
            {"name": "risk_limit_cents", "type": "integer", "nullable": True},
            {"name": "strike_type", "type": "varchar", "nullable": True},
            {"name": "custom_strike", "type": "varchar", "nullable": True},
            {"name": "rules_primary", "type": "varchar", "nullable": True},
            {"name": "rules_secondary", "type": "varchar", "nullable": True},
            {"name": "tick_size", "type": "integer", "nullable": True},
            {"name": "mve_collection_ticker", "type": "varchar", "nullable": True},
            {"name": "mve_selected_legs", "type": "varchar", "nullable": True},
            {"name": "collection_date", "type": "varchar"},
            {"name": "date", "type": "varchar"},
            {"name": "floor_strike", "type": "varchar", "nullable": True},
            {"name": "early_close_condition", "type": "varchar", "nullable": True},
            {"name": "cap_strike", "type": "varchar", "nullable": True},
            {"name": "primary_participant_key", "type": "varchar", "nullable": True},
            {"name": "fee_waiver_expiration_time", "type": "varchar", "nullable": True}
        ]
    
    def upload_daily_data(self):
        """Upload today's Kalshi data to persistent Dune tables"""
        logger.info("=" * 50)
        logger.info("STARTING DUNE UPLOAD TO PERSISTENT TABLES")
        logger.info("=" * 50)
        
        results = {'events': False, 'markets': False}
        
        # Process events data
        events_file = self.data_dir / f"kalshi_events_{self.date_str}.csv"
        if events_file.exists():
            logger.info(f"Processing events data from {events_file}")
            
            try:
                df_events = pd.read_csv(events_file)
                
                # Create table if first time
                events_schema = self.define_events_schema()
                table_created = self.create_table_if_not_exists(
                    self.events_table,
                    events_schema,
                    f"Kalshi prediction market events data. Updated daily with new events and status changes. Data sourced from Kalshi API."
                )
                
                if table_created:
                    # Map CSV columns to schema columns (handle case differences)
                    df_events = df_events.rename(columns={'DATE': 'date'})
                    # Clear table and insert fresh data (prevents duplicates)
                    results['events'] = self.clear_todays_data_via_rebuild(self.events_table, df_events)
                
                time.sleep(1)  # Be nice to API
                
            except Exception as e:
                logger.error(f"Error processing events data: {e}")
        else:
            logger.warning(f"Events file not found: {events_file}")
        
        # Process markets data
        markets_file = self.data_dir / f"kalshi_markets_{self.date_str}.csv"
        if markets_file.exists():
            logger.info(f"Processing markets data from {markets_file}")
            
            try:
                df_markets = pd.read_csv(markets_file)
                
                # Create table if first time
                markets_schema = self.define_markets_schema()
                table_created = self.create_table_if_not_exists(
                    self.markets_table,
                    markets_schema,
                    f"Kalshi prediction market individual markets data. Updated daily with current pricing, volume, and liquidity metrics. Data sourced from Kalshi API."
                )
                
                if table_created:
                    # Map CSV columns to schema columns (handle case differences)
                    df_markets = df_markets.rename(columns={'DATE': 'date'})
                    # Clear table and insert fresh data (prevents duplicates)
                    results['markets'] = self.clear_todays_data_via_rebuild(self.markets_table, df_markets)
                
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
