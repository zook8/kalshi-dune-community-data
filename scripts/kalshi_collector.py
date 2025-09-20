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
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            # Respect rate limit per docs (especially public): 1.5s/req for bulk is very safe.
            time.sleep(1.5)
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            return None

    def get_all_paginated_data(self, endpoint, params=None, api_limit=200, result_key=None):
        """Generic paginated fetch for endpoints. result_key='events' or 'markets'."""
        data = []
        cursor = None
        page = 1
        if params is None:
            params = {}
        params['limit'] = api_limit
        while True:
            if cursor:
                params['cursor'] = cursor
            logger.info(f"Fetching {endpoint} - Page {page}")
            response = self.make_request(endpoint, params)
            if not response:
                logger.error(f"Failed to fetch page {page} for {endpoint}")
                break
            key = result_key or next((k for k in ['events', 'markets'] if k in response), None)
            if not key:
                logger.error(f"Unexpected response: missing expected data list for {endpoint}")
                break
            items = response[key]
            if not items:
                logger.info(f"No data returned for {endpoint} page {page}")
                break
            data.extend(items)
            logger.info(f"Page {page}: {len(items)} records, Total: {len(data)}")
            cursor = response.get('cursor')
            if not cursor:
                logger.info(f"Completed fetching {endpoint}: {len(data)} total records")
                break
            page += 1
            if page > 50:
                logger.warning(f"Stopped at page {page} - too many pages, possible bug.")
                break
        return data

    def collect_events_data(self):
        """Collect all open events data with all columns."""
        logger.info("Starting open events data collection...")
        events = self.get_all_paginated_data('/events', {'status': 'open'}, api_limit=200, result_key='events')
        for event in events:
            # Add explicit collection datetime fields
            event['collection_date'] = self.collection_datetime.isoformat()
            event['DATE'] = self.collection_date
        logger.info(f"Collected {len(events)} open events")
        return events

    def collect_markets_data(self):
        """Collect all open markets data with all columns."""
        logger.info("Starting open markets data collection...")
        markets = self.get_all_paginated_data('/markets', {'status': 'open'}, api_limit=1000, result_key='markets')
        for market in markets:
            market['collection_date'] = self.collection_datetime.isoformat()
            market['DATE'] = self.collection_date
        logger.info(f"Collected {len(markets)} open markets")
        return markets

    def save_to_csv(self, data, filename):
        """Save data to CSV file"""
        if not data:
            logger.warning(f"No data to save for {filename}")
            return None
        filepath = self.data_dir / f"{filename}_{self.date_str}.csv"
        try:
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {len(data)} records to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save {filename}: {e}")
            return None

    def run_daily_collection(self):
        logger.info("=" * 50)
        logger.info("STARTING KALSHI OPEN EVENTS + MARKETS COLLECTION")
        logger.info("=" * 50)
        start_time = datetime.now()
        try:
            events = self.collect_events_data()
            events_file = self.save_to_csv(events, 'kalshi_events')
            markets = self.collect_markets_data()
            markets_file = self.save_to_csv(markets, 'kalshi_markets')
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info("=" * 50)
            logger.info("COLLECTION COMPLETE")
            logger.info(f"Events collected: {len(events)}")
            logger.info(f"Markets collected: {len(markets)}")
            logger.info(f"Duration: {duration}")
            logger.info(f"Files saved:")
            if events_file:
                logger.info(f"  - {events_file}")
            if markets_file:
                logger.info(f"  - {markets_file}")
            logger.info("=" * 50)
            return {
                'success': True,
                'events_count': len(events),
                'markets_count': len(markets),
                'events_file': str(events_file) if events_file else None,
                'markets_file': str(markets_file) if markets_file else None,
                'duration': str(duration)
            }
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            return {'success': False, 'error': str(e)}

if __name__ == "__main__":
    collector = KalshiCollector()
    result = collector.run_daily_collection()
    if result['success']:
        logger.info("Daily collection completed successfully")
        sys.exit(0)
    else:
        logger.error(f"Daily collection failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)
