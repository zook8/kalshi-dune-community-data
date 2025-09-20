#!/usr/bin/env python3
"""
Dune Uploader - Upload Kalshi data to Dune Analytics
Uses Dune API to create community datasets for Kalshi prediction market data
"""

import os
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
from dotenv import load_dotenv
from dune_client import DuneClient
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
        
        self.dune_client = DuneClient.from_env()
        self.data_dir = PROJECT_ROOT / "data"
        self.date_str = datetime.now().strftime('%Y%m%d')
    
    def prepare_data_for_upload(self, csv_file_path):
        """Prepare CSV data for Dune upload"""
        try:
            df = pd.read_csv(csv_file_path)
            
            # Convert DataFrame back to CSV string for Dune API
            csv_string = df.to_csv(index=False)
            
            logger.info(f"Prepared {len(df)} rows from {csv_file_path}")
            return csv_string, len(df)
            
        except Exception as e:
            logger.error(f"Failed to prepare data from {csv_file_path}: {e}")
            return None, 0
    
    def upload_to_dune(self, csv_data, table_name, description):
        """Upload CSV data to Dune"""
        try:
            result = self.dune_client.upload_csv(
                data=csv_data,
                table_name=table_name,
                description=description,
                is_private=False  # Make data public for community use
            )
            
            if result:
                logger.info(f"Successfully uploaded to Dune table: {table_name}")
                return True
            else:
                logger.error(f"Failed to upload {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Dune upload failed for {table_name}: {e}")
            return False
    
    def upload_daily_data(self):
        """Upload today's Kalshi data to Dune"""
        logger.info("=" * 50)
        logger.info("STARTING DUNE UPLOAD")
        logger.info("=" * 50)
        
        results = {'events': False, 'markets': False}
        
        # Upload events data
        events_file = self.data_dir / f"kalshi_events_{self.date_str}.csv"
        if events_file.exists():
            logger.info(f"Uploading events data from {events_file}")
            csv_data, row_count = self.prepare_data_for_upload(events_file)
            
            if csv_data:
                table_name = f"kalshi_events_{self.date_str}"
                description = f"Kalshi prediction market events data collected on {datetime.now().strftime('%Y-%m-%d')}. Contains {row_count} events with full metadata including status, category, title, and market counts. Data collected via Kalshi API."
                
                results['events'] = self.upload_to_dune(csv_data, table_name, description)
                
                # Wait between uploads to be respectful
                time.sleep(2)
        else:
            logger.warning(f"Events file not found: {events_file}")
        
        # Upload markets data
        markets_file = self.data_dir / f"kalshi_markets_{self.date_str}.csv"
        if markets_file.exists():
            logger.info(f"Uploading markets data from {markets_file}")
            csv_data, row_count = self.prepare_data_for_upload(markets_file)
            
            if csv_data:
                table_name = f"kalshi_markets_{self.date_str}"
                description = f"Kalshi prediction market individual markets data collected on {datetime.now().strftime('%Y-%m-%d')}. Contains {row_count} open markets with pricing, volume, liquidity, and trading data. Data collected via Kalshi API."
                
                results['markets'] = self.upload_to_dune(csv_data, table_name, description)
        else:
            logger.warning(f"Markets file not found: {markets_file}")
        
        # Summary
        logger.info("=" * 50)
        logger.info("UPLOAD COMPLETE")
        logger.info(f"Events upload: {'SUCCESS' if results['events'] else 'FAILED'}")
        logger.info(f"Markets upload: {'SUCCESS' if results['markets'] else 'FAILED'}")
        
        # Log table access info
        if results['events'] or results['markets']:
            logger.info("\nTo query your data in Dune, use:")
            if results['events']:
                logger.info(f"SELECT * FROM dune.{self.dune_client.get_username() or 'your_username'}.kalshi_events_{self.date_str}")
            if results['markets']:
                logger.info(f"SELECT * FROM dune.{self.dune_client.get_username() or 'your_username'}.kalshi_markets_{self.date_str}")
        
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
