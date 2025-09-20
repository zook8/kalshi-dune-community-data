# Kalshi Dune Pipeline

Automated data collection pipeline for Kalshi prediction market data, uploading to Dune Analytics for analysis.

## Overview

This pipeline collects daily data from the Kalshi API and uploads it to persistent Dune tables with built-in duplicate prevention.

## Components

- **`run_pipeline.py`**: Main orchestrator script
- **`scripts/kalshi_collector.py`**: Collects events and markets data from Kalshi API
- **`scripts/dune_uploader.py`**: Uploads data to Dune with duplicate prevention
- **GitHub Actions**: Automated daily execution

## Features

- ✅ **Duplicate Prevention**: Clear-and-replace strategy ensures no duplicate data
- ✅ **Daily Automation**: GitHub Actions runs pipeline daily at 12:00 UTC
- ✅ **Comprehensive Data**: Collects all open events and markets with full metadata
- ✅ **Error Handling**: Robust API error handling and retry logic
- ✅ **Secure**: API keys managed via GitHub Secrets

## Setup

### Local Development

1. Clone the repository
2. Create virtual environment: `python -m venv venv`
3. Activate: `source venv/bin/activate` (Linux/Mac) or `venv\Scripts\activate` (Windows)
4. Install dependencies: `pip install -r requirements.txt`
5. Create `.env` file with your API keys:
   ```
   DUNE_API_KEY=your_dune_api_key_here
   ```
6. Run manually: `python run_pipeline.py`

### GitHub Actions Setup

The pipeline runs automatically via GitHub Actions. API keys are stored securely in GitHub Secrets.

## Dune Tables

- **Events**: `dune.ghost_in_the_code.kalshi_events`
- **Markets**: `dune.ghost_in_the_code.kalshi_markets`

Each table contains the latest data collection with guaranteed no duplicates.

## Usage

Query the data in Dune Analytics:

```sql
-- Get latest events
SELECT * FROM dune.ghost_in_the_code.kalshi_events
ORDER BY collection_date DESC

-- Get latest markets with pricing
SELECT ticker, title, yes_bid, yes_ask, volume
FROM dune.ghost_in_the_code.kalshi_markets
WHERE yes_bid IS NOT NULL
ORDER BY volume DESC
```

## Architecture

1. **Collection**: Kalshi API → CSV files
2. **Upload**: CSV → Dune persistent tables (clear-and-replace)
3. **Schedule**: Daily at 12:00 UTC via GitHub Actions
4. **Analysis**: Query data directly in Dune Analytics

## Status

🟢 **Active** - Pipeline running daily with duplicate prevention