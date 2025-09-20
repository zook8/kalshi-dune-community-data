#!/usr/bin/env python3
"""
Kalshi Data Collector - Public API Version (No Authentication Required)
Collects all active events and markets using public endpoints only
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

# Load environment variables (only need DUNE_API_KEY now)
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

class KalshiPublicCollector:
    def __init__(self):
        # No API key required! Using public endpoints
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.session = requests.Session()
        
        # Set reasonable headers to be a good API citizen
        self.session.headers.update({
            'User-Agent': 'KalshiDunePublicCollector/1.0',
            'Accept': 'application/json'
        })
        
        # Create data directories
        self.data_dir = PROJECT_ROOT / "data"
        self.data_dir.mkdir(exist_ok=True)
        
        # Today's date for file naming
        self.date_str = datetime.now().strftime('%Y%m%d')
        
        # Rate limiting - be extra conservative since we're unauthenticated
        self.request_delay = 3.0  # 3 seconds between requests = 20 requests/minute
        
    def make_request(self, endpoint, params=None):
        """Make API request with error handling and conservative rate limiting"""
        try:
            url = f"{self.base_url}{endpoint}"
            
            # Conservative rate limiting for public endpoints
            logger.info(f"Making request to {endpoint} (waiting {self.request_delay}s for rate limit)")
            time.sleep(self.request_delay)
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            return None
    
    def get_all_paginated_data(self, endpoint, params=None):
        """Fetch all data from paginated endpoint with conservative rate limiting"""
        all_data = []
        cursor = None
        page = 1
        
        if params is None:
            params = {}
        
        # Use moderate limit to balance efficiency with rate limits
        params['limit'] = 200  # Conservative limit for public endpoints
        
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
                cursor = response.get('cursor')
            elif 'markets' in response:
                page_data = response['markets']
                cursor = response.get('cursor')
            elif 'trades' in response:
                page_data = response['trades']
                cursor = response.get('cursor')
            else:
                logger.error(f"Unexpected response structure for {endpoint}")
                break
            
            if not page_data:
                logger.info(f"No data returned for {endpoint} page {page}")
                break
            
            all_data.extend(page_data)
            logger.info(f"Page {page}: {len(page_data)} records, Total: {len(all_data)}")
            
            # Check for next page
            if not cursor:
                logger.info(f"Completed fetching {endpoint}: {len(all_data)} total records")
                break
            
            page += 1
            
            # Safety check to avoid infinite loops
            if page > 20:
                logger.warning(f"Stopped at page {page} - reached safety limit")
                break
        
        return all_data
    
    def collect_events_data(self):
        """Collect all events data using public API"""
        logger.info("Starting events data collection (public API)...")
        
        # Get open events - this is our primary interest
        logger.info("Fetching open events...")
        open_events = self.get_all_paginated_data('/events', {
            'status': 'open',
            'with_nested_markets': True  # Get market data embedded in events
        })
        
        if not open_events:
            logger.error("No open events found")
            return []
        
        # Add collection metadata
        for event in open_events:
            event['collection_date'] = datetime.now(timezone.utc).isoformat()
            event['collection_timestamp'] = int(datetime.now().timestamp())
        
        logger.info(f"Collected {len(open_events)} open events")
        return open_events
    
    def collect_markets_data(self):
        """Collect markets data using public API (alternative method)"""
        logger.info("Starting markets data collection (public API)...")
        
        # Get all open markets directly
        logger.info("Fetching open markets...")
        open_markets = self.get_all_paginated_data('/markets', {
            'status': 'open'
        })
        
        if not open_markets:
            logger.error("No open markets found")
            return []
        
        # Add collection metadata
        for market in open_markets:
            market['collection_date'] = datetime.now(timezone.utc).isoformat()
            market['collection_timestamp'] = int(datetime.now().timestamp())
        
        logger.info(f"Collected {len(open_markets)} open markets")
        return open_markets
    
    def collect_trade_data_sample(self):
        """Collect recent trade data for market activity insights"""
        logger.info("Starting recent trade data collection...")
        
        # Get recent trades (last hour)
        from datetime import timedelta
        hour_ago = int((datetime.now() - timedelta(hours=1)).timestamp())
        
        recent_trades = self.get_all_paginated_data('/markets/trades', {
            'min_ts': hour_ago,
            'limit': 100  # Just a sample for activity metrics
        })
        
        if recent_trades:
            # Add collection metadata
            for trade in recent_trades:
                trade['collection_date'] = datetime.now(timezone.utc).isoformat()
                trade['collection_timestamp'] = int(datetime.now().timestamp())
        
        logger.info(f"Collected {len(recent_trades) if recent_trades else 0} recent trades")
        return recent_trades or []
    
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
        """Execute daily data collection using public APIs only"""
        logger.info("=" * 50)
        logger.info("STARTING KALSHI PUBLIC API DATA COLLECTION")
        logger.info("=" * 50)
        
        start_time = datetime.now()
        
        try:
            # Test API connectivity first
            logger.info("Testing API connectivity...")
            status_response = self.make_request('/exchange/status')
            if not status_response:
                raise Exception("Cannot connect to Kalshi API")
            
            logger.info(f"Exchange status: Trading Active = {status_response.get('trading_active', 'Unknown')}")
            
            # Collect events data (includes nested market data)
            events = self.collect_events_data()
            events_file = self.save_to_csv(events, 'kalshi_events')
            
            # Collect markets data separately for completeness
            markets = self.collect_markets_data()
            markets_file = self.save_to_csv(markets, 'kalshi_markets')
            
            # Collect sample trade data for activity insights
            trades = self.collect_trade_data_sample()
            trades_file = self.save_to_csv(trades, 'kalshi_recent_trades') if trades else None
            
            # Summary
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("=" * 50)
            logger.info("PUBLIC API COLLECTION COMPLETE")
            logger.info(f"Events collected: {len(events) if events else 0}")
            logger.info(f"Markets collected: {len(markets) if markets else 0}")
            logger.info(f"Recent trades collected: {len(trades) if trades else 0}")
            logger.info(f"Duration: {duration}")
            logger.info(f"API calls made: ~{3 + len(events)//200 + len(markets)//200}")  # Rough estimate
            logger.info(f"Files saved:")
            if events_file:
                logger.info(f"  - {events_file}")
            if markets_file:
                logger.info(f"  - {markets_file}")
            if trades_file:
                logger.info(f"  - {trades_file}")
            logger.info("=" * 50)
            
            return {
                'success': True,
                'events_count': len(events) if events else 0,
                'markets_count': len(markets) if markets else 0,
                'trades_count': len(trades) if trades else 0,
                'events_file': str(events_file) if events_file else None,
                'markets_file': str(markets_file) if markets_file else None,
                'trades_file': str(trades_file) if trades_file else None,
                'duration': str(duration)
            }
            
        except Exception as e:
            logger.error(f"Collection failed: {e}")
            return {'success': False, 'error': str(e)}

if __name__ == "__main__":
    collector = KalshiPublicCollector()
    result = collector.run_daily_collection()
    
    if result['success']:
        logger.info("Daily public API collection completed successfully")
        sys.exit(0)
    else:
        logger.error(f"Daily collection failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)
