# Codebase Structure

## Directory Layout
```
kalshi-dune-pipeline/
├── .github/workflows/
│   └── daily-kalshi-pipeline.yml    # GitHub Actions workflow
├── scripts/
│   ├── run_pipeline.py              # Main orchestrator
│   ├── kalshi_collector.py          # Kalshi API data collection
│   ├── dune_uploader.py            # Dune Analytics upload with duplicate prevention
│   └── preview_data.py             # Data inspection utility
├── config/
│   └── logrotate.conf              # Log rotation configuration
├── data/                           # CSV files (gitignored)
├── logs/                           # Log files (gitignored)
├── requirements.txt                # Python dependencies
├── .env.example                   # Environment variables template
├── .gitignore                     # Git ignore rules
└── README.md                      # Project documentation
```

## Core Components

### scripts/run_pipeline.py
- **Purpose**: Main orchestrator that coordinates collection and upload
- **Key Functions**: `run_script()`, `main()`
- **Flow**: Executes collector → uploader → logs results

### scripts/kalshi_collector.py
- **Purpose**: Collects data from Kalshi Trade API v2
- **Main Class**: `KalshiCollector`
- **Key Methods**: 
  - `collect_events_data()` - Gets open events
  - `collect_markets_data()` - Gets open markets
  - `save_to_csv()` - Saves data to timestamped CSV files
- **Features**: Rate limiting (1.5s between requests), pagination handling

### scripts/dune_uploader.py
- **Purpose**: Uploads data to Dune with bulletproof duplicate prevention
- **Main Class**: `DuneUploader`
- **Key Methods**:
  - `clear_table_completely()` - Clears all table data
  - `clear_todays_data_via_rebuild()` - Clear-and-replace strategy
  - `define_events_schema()` - 13-column events schema
  - `define_markets_schema()` - 57-column comprehensive markets schema
- **Duplicate Prevention**: Uses Dune's `/clear` API before each insert