# Suggested Commands for Development

## Environment Setup
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment template
cp .env.example .env
# Edit .env with your DUNE_API_KEY
```

## Running the Pipeline
```bash
# Run complete pipeline (collection + upload)
python scripts/run_pipeline.py

# Run individual components
python scripts/kalshi_collector.py    # Collection only
python scripts/dune_uploader.py       # Upload only (requires CSV files)

# Preview collected data
python scripts/preview_data.py
```

## Development Workflow
```bash
# Check git status
git status

# Add and commit changes
git add .
git commit -m "Description of changes"

# Push to GitHub
git push origin main

# Create feature branch
git checkout -b feature/new-feature
```

## Debugging and Logs
```bash
# View today's logs
tail -f logs/run_pipeline_$(date +%Y%m%d).log
tail -f logs/kalshi_collector_$(date +%Y%m%d).log
tail -f logs/dune_uploader_$(date +%Y%m%d).log

# Check data files
ls -la data/
head data/kalshi_events_$(date +%Y%m%d).csv
head data/kalshi_markets_$(date +%Y%m%d).csv
```

## GitHub Actions
```bash
# Trigger manual run (requires GitHub CLI)
gh workflow run "Daily Kalshi Data Pipeline"

# View workflow status
gh run list --workflow="Daily Kalshi Data Pipeline"

# View logs for failed runs
gh run view <run_id> --log
```

## Data Validation
```bash
# Check CSV file integrity
python -c "import pandas as pd; df=pd.read_csv('data/kalshi_events_$(date +%Y%m%d).csv'); print(f'Events: {len(df)} rows, {len(df.columns)} columns')"

python -c "import pandas as pd; df=pd.read_csv('data/kalshi_markets_$(date +%Y%m%d).csv'); print(f'Markets: {len(df)} rows, {len(df.columns)} columns')"
```

## Testing Commands
```bash
# Manual pipeline test
python scripts/run_pipeline.py

# Test Kalshi API connectivity
python -c "import requests; r=requests.get('https://api.elections.kalshi.com/trade-api/v2/exchange/status'); print(r.json())"

# Test environment variables
python -c "import os; print('DUNE_API_KEY:', 'SET' if os.getenv('DUNE_API_KEY') else 'NOT SET')"
```