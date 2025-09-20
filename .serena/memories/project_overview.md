# Kalshi Dune Pipeline - Project Overview

## Purpose
Automated data collection pipeline that gathers Kalshi prediction market data daily and uploads it to Dune Analytics for community analysis. The pipeline ensures no duplicate data through a clear-and-replace strategy.

## Tech Stack
- **Language**: Python 3.11+
- **Core Libraries**: 
  - pandas>=2.0.0 (data manipulation)
  - requests>=2.31.0 (API calls)
  - python-dotenv>=1.0.0 (environment variables)
- **APIs**: Kalshi Trade API v2, Dune Analytics API v1
- **Platform**: GitHub Actions (Ubuntu latest)
- **Data Storage**: Dune Analytics persistent tables

## Architecture
1. **Data Collection**: `kalshi_collector.py` fetches open events and markets from Kalshi API
2. **Data Processing**: Adds collection timestamps and formats for Dune compatibility
3. **Data Upload**: `dune_uploader.py` clears existing data and uploads fresh daily data
4. **Automation**: GitHub Actions runs daily at 12:00 UTC
5. **Storage**: Data stored in persistent Dune tables accessible via SQL

## Key Features
- **Duplicate Prevention**: Clear-and-replace strategy guarantees no duplicates
- **Comprehensive Schema**: 57-column markets schema handles all CSV data
- **Error Handling**: Robust API retry logic and proper error logging
- **Security**: API keys managed via GitHub Secrets (.env for local development)
- **Monitoring**: Detailed logging with artifacts uploaded on failure

## Dune Tables Created
- `dune.ghost_in_the_code.kalshi_events` - Event data with metadata
- `dune.ghost_in_the_code.kalshi_markets` - Market data with pricing and volume