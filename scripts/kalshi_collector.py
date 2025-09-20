#!/usr/bin/env python3
"""
Kalshi Data Collector - Daily collection of open events and open markets data
Each record gets a 'collection_date' (full ISO) and 'DATE' (YYYY-MM-DD).
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import logging
import time

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

# Ensure logs directory exists
(PROJECT_ROOT / "logs").mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / f"kalshi_collector_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KalshiCollector:
    def __init__(self):
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'KalshiDuneCollector/2.0',
            'Accept': 'application/json'
        })
        self.data_dir = PROJECT_ROOT / "data"
        self.data_dir.mkdir(exist_ok=True)
        self.date_str = datetime.now().strftime('%Y%m%d')
        self.collection_datetime = datetime.now(timezone.utc)
        self.collection_date = self.collection_datetime.date().isoformat()  # YYYY-MM-DD

    def make_request(self, endpoint, params=None, api_limit=200):
        """Make API request with error handling and gentle rate limiting"""
        try:
            url = f"{self.base_url}/{endpoint}"
            logger.info(f"Making request to: {url}")

            if params is None:
                params = {}

            # Add limit parameter
            params['limit'] = api_limit

            response = self.session.get(url, params=params)
            response.raise_for_status()

            # Rate limiting - be gentle with public API
            time.sleep(1.5)

            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            return None

    def get_all_events(self):
        """Fetch all open events with pagination"""
        logger.info("Fetching all open events...")
        all_events = []
        cursor = None
        page = 1

        while True:
            logger.info(f"Fetching events page {page}...")

            params = {'status': 'open'}
            if cursor:
                params['cursor'] = cursor

            data = self.make_request('events', params)
            if not data:
                logger.error("Failed to fetch events")
                break

            events = data.get('events', [])
            if not events:
                logger.info("No more events found")
                break

            # Add metadata to each event
            for event in events:
                event['collection_datetime'] = self.collection_datetime.isoformat()
                event['collection_date'] = self.collection_date
                event['DATE'] = self.collection_date  # For Dune compatibility

            all_events.extend(events)
            logger.info(f"Collected {len(events)} events from page {page}")

            # Check for pagination
            cursor = data.get('cursor')
            if not cursor:
                logger.info("No more pages available")
                break

            page += 1

        logger.info(f"Total events collected: {len(all_events)}")
        return all_events

    def get_all_markets(self):
        """Fetch all open markets with pagination"""
        logger.info("Fetching all open markets...")
        all_markets = []
        cursor = None
        page = 1

        while True:
            logger.info(f"Fetching markets page {page}...")

            params = {'status': 'open'}
            if cursor:
                params['cursor'] = cursor

            data = self.make_request('markets', params)
            if not data:
                logger.error("Failed to fetch markets")
                break

            markets = data.get('markets', [])
            if not markets:
                logger.info("No more markets found")
                break

            # Add metadata to each market
            for market in markets:
                market['collection_datetime'] = self.collection_datetime.isoformat()
                market['collection_date'] = self.collection_date
                market['DATE'] = self.collection_date  # For Dune compatibility

            all_markets.extend(markets)
            logger.info(f"Collected {len(markets)} markets from page {page}")

            # Check for pagination
            cursor = data.get('cursor')
            if not cursor:
                logger.info("No more pages available")
                break

            page += 1

        logger.info(f"Total markets collected: {len(all_markets)}")
        return all_markets

    def save_to_csv(self, data, filename):
        """Save data to CSV file"""
        if not data:
            logger.warning(f"No data to save for {filename}")
            return False

        filepath = self.data_dir / f"{filename}_{self.date_str}.csv"

        try:
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {len(data)} records to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to save {filename}: {e}")
            return False

    def run_collection(self):
        """Run the complete data collection process"""
        logger.info("=" * 50)
        logger.info("STARTING KALSHI DATA COLLECTION")
        logger.info(f"Collection Date: {self.collection_date}")
        logger.info("=" * 50)

        success = True

        # Collect events
        try:
            events = self.get_all_events()
            if events:
                self.save_to_csv(events, "kalshi_events")
            else:
                logger.error("No events collected")
                success = False
        except Exception as e:
            logger.error(f"Events collection failed: {e}")
            success = False

        # Collect markets
        try:
            markets = self.get_all_markets()
            if markets:
                self.save_to_csv(markets, "kalshi_markets")
            else:
                logger.error("No markets collected")
                success = False
        except Exception as e:
            logger.error(f"Markets collection failed: {e}")
            success = False

        if success:
            logger.info("=" * 50)
            logger.info("KALSHI DATA COLLECTION COMPLETED SUCCESSFULLY")
            logger.info("=" * 50)
        else:
            logger.error("=" * 50)
            logger.error("KALSHI DATA COLLECTION COMPLETED WITH ERRORS")
            logger.error("=" * 50)

        return success

def main():
    collector = KalshiCollector()
    success = collector.run_collection()
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)