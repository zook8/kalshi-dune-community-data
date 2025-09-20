#!/usr/bin/env python3
"""
Kalshi Data Collector - Daily collection of events and markets data
Collects all active events and their associated markets, preserving original API schema
"""

import os
import sys
import requests
import json
import csv
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import logging
from dotenv import load_dotenv
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
        logging.FileHandler(PROJECT_ROOT / "logs" / f"kalshi_collector_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KalshiCollector:
    def __init__(self):
        self.api_key = os.getenv('KALSHI_API_KEY')
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({'Authorization': f'Bearer {self.api_key}'})
        
        # Create data directories
        self.data_dir = PROJECT_ROOT / "data"
        self.data_dir.mkdir(exist_ok=True)
        
        # Today's date for file naming
        self.date_str = datetime.now().strftime('%Y%m%d')
        
    def make_request(self, endpoint, params=None):
        """Make API request with error handling and rate limiting"""
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # Basic rate limiting - be nice to the API
            time.sleep(0.1)  # 100ms between requests
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            return None
    
    def get_all_paginated_data(self, endpoint, params=None):
        """Fetch all data from paginated endpoint"""
        all_data = []
        cursor = None
        page = 1
        
        if params is None:
            params = {}
        
        # Set high limit to minimize API calls
        params['limit'] = 1000
        
        while True:
            if cursor:
                params['cursor'] = cursor
            
            logger.info(f"Fetching {endpoint} - Page {page}")
            response = self.make_request(endpoint, params)
            
            if not response:
                logger.error(f"Failed to fetch page {page} for {endpoint}")
                break
            
            # Different endpoints return data in different keys
            if 'events' in response:
                page_data = response['events']
            elif 'markets' in response:
                page_data = response['markets']
            else:
                logger.error(f"Unexpected response structure for {endpoint}")
                break
            
            all_data.extend(page_data)
            logger.info(f"Page {page}: {len(page_data)} records, Total: {len(all_data)}")
            
            # Check for next page
            cursor = response.get('cursor')
            if not cursor:
                logger.info(f"Completed fetching {endpoint}: {len(all_data)} total records")
                break
            
            page += 1
            
            # Safety check - shouldn't need this many pages for daily data
            if page > 50:
                logger.warning(f"Stopped at page {page} - too many pages, possible infinite loop")
                break
        
        return all_data
    
    def collect_events_data(self):
        """Collect all events data"""
        logger.info("Starting events data collection...")
        
        # Get all events (we want both open and recent closed events)
        all_events = []
        
        # Get open events
        open_events = self.get_all_paginated_data('/events', {'status': 'open'})
        if open_events:
            all_events.extend(open_events)
        
        # Get recently closed events (last 7 days)
        from datetime import timedelta
        week_ago = int((datetime.now() - timedelta(days=7)).timestamp())
        recent_events = self.get_all_paginated_data('/events', {
            'status': 'closed',
            'min_close_ts': week_ago
        })
        if recent_events:
            all_events.extend(recent_events)
        
        # Remove duplicates by event_ticker
        unique_events = {event['event_ticker']: event for event in all_events}.values()
        events_list = list(unique_events)
        
        logger.info(f"Collected {len(events_list)} unique events")
        
        # Add collection metadata
        for event in events_list:
            event['collection_date'] = datetime.now(timezone.utc).isoformat()
            event['collection_timestamp'] = int(datetime.now().timestamp())
        
        return events_list
    
    def collect_markets_data(self, events):
        """Collect markets data for all events"""
        logger.info("Starting markets data collection...")
        
        all_markets = []
        
        # Collect markets for open events
        open_events = [e for e in events if e.get('status') == 'open']
        logger.info(f"Collecting markets for {len(open_events)} open events")
        
        for event in open_events:
            event_ticker = event['event_ticker']
            logger.info(f"Fetching markets for event: {event_ticker}")
            
            markets = self.get_all_paginated_data('/markets', {
                'event_ticker': event_ticker,
                'status': 'open'  # Only collect open markets as requested
            })
            
            if markets:
                # Add event context to each market
                for market in markets:
                    market['collection_date'] = datetime.now(timezone.utc).isoformat()
                    market['collection_timestamp'] = int(datetime.now().timestamp())
                
                all_markets.extend(markets)
                logger.info(f"  → {len(markets)} markets collected")
            else:
                logger.warning(f"  → No markets found for {event_ticker}")
        
        logger.info(f"Collected {len(all_markets)} total markets")
        return all_markets
    
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
        """Execute daily data collection"""
        logger.info("=" * 50)
        logger.info("STARTING KALSHI DAILY DATA COLLECTION")
        logger.info("=" * 50)
        
        start_time = datetime.now()
        
        try:
            # Collect events data
            events = self.collect_events_data()
            events_file = self.save_to_csv(events, 'kalshi_events')
            
            # Collect markets data
            markets = self.collect_markets_data(events)
            markets_file = self.save_to_csv(markets, 'kalshi_markets')
            
            # Summary
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("=" * 50)
            logger.info("COLLECTION COMPLETE")
            logger.info(f"Events collected: {len(events) if events else 0}")
            logger.info(f"Markets collected: {len(markets) if markets else 0}")
            logger.info(f"Duration: {duration}")
            logger.info(f"Files saved:")
            if events_file:
                logger.info(f"  - {events_file}")
            if markets_file:
                logger.info(f"  - {markets_file}")
            logger.info("=" * 50)
            
            return {
                'success': True,
                'events_count': len(events) if events else 0,
                'markets_count': len(markets) if markets else 0,
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
