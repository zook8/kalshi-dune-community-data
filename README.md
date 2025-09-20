# Kalshi-Dune Community Data Pipeline

Automated daily pipeline for collecting Kalshi prediction market data and uploading to Dune Analytics for community analysis.

## Overview

This project provides an automated data pipeline that:
- Collects prediction market data from Kalshi API
- Processes and formats the data for analytics
- Uploads the data to Dune Analytics for community dashboards and analysis

## Project Structure

```
kalshi-dune-community-data/
├── .github/workflows/
│   └── daily-pipeline.yml          # GitHub Actions workflow for daily automation
├── scripts/
│   ├── kalshi_collector.py         # Kalshi API data collection
│   ├── dune_uploader.py            # Dune Analytics data upload
│   └── run_pipeline.py             # Main pipeline orchestrator
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── .env.example                   # Environment variables template
└── .gitignore                     # Git ignore rules
```

## Setup

### 1. Clone the Repository
```bash
git clone https://github.com/zook8/kalshi-dune-community-data.git
cd kalshi-dune-community-data
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Configuration
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your API keys
nano .env
```

Required environment variables:
- `DUNE_API_KEY`: Your Dune Analytics API key
- `KALSHI_API_KEY`: Your Kalshi API key (if required)

### 4. Manual Execution
```bash
cd scripts
python run_pipeline.py
```

## Automated Execution

The pipeline runs automatically via GitHub Actions:
- **Schedule**: Daily at 6 AM UTC
- **Manual Trigger**: Available via GitHub Actions interface
- **Environment**: Uses repository secrets for API keys

### Repository Secrets Required
- `DUNE_API_KEY`: Set in GitHub repository settings → Secrets

## Scripts Description

### kalshi_collector.py
Handles data collection from Kalshi's prediction market API:
- Fetches market data, predictions, and trading activity
- Processes and formats data for analysis
- Implements rate limiting and error handling

### dune_uploader.py
Manages data upload to Dune Analytics:
- Authenticates with Dune API
- Uploads processed data to designated tables
- Handles data validation and error recovery

### run_pipeline.py
Main orchestrator that:
- Coordinates the entire data pipeline
- Manages logging and error handling
- Ensures data consistency and completeness

## Data Tables

The pipeline creates/updates the following Dune tables:
- `kalshi_markets`: Market information and metadata
- `kalshi_trades`: Trading activity and volume data
- `kalshi_predictions`: Prediction outcomes and probabilities

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions:
- Open an issue on GitHub
- Check the GitHub Actions logs for pipeline errors
- Review the Dune Analytics dashboard for data validation