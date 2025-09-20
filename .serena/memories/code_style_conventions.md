# Code Style and Conventions

## Python Style Guidelines

### Naming Conventions
- **Classes**: PascalCase (`KalshiCollector`, `DuneUploader`)
- **Functions/Methods**: snake_case (`collect_events_data`, `upload_daily_data`)
- **Variables**: snake_case (`events_file`, `df_markets`, `collection_date`)
- **Constants**: UPPER_SNAKE_CASE (`PROJECT_ROOT`, `DUNE_API_KEY`)
- **Files**: snake_case (`kalshi_collector.py`, `dune_uploader.py`)

### Documentation
- **Module docstrings**: Triple-quoted strings at file top describing purpose
- **Class docstrings**: Brief description of class purpose
- **Method docstrings**: Description of method purpose and behavior
- **Comments**: Explain complex logic, API interactions, and business rules

### Code Organization
- **Imports**: Standard library → Third party → Local imports
- **Constants**: Defined at module level after imports
- **Classes**: One main class per file with descriptive name
- **Methods**: Logical grouping within classes, public methods first
- **Error Handling**: Comprehensive try/catch with specific logging

### Logging
- **Format**: `%(asctime)s - %(levelname)s - %(message)s`
- **Levels**: INFO for normal flow, ERROR for failures, WARNING for issues
- **Files**: Daily rotation with timestamp in filename
- **Console**: Simultaneous output to stdout for debugging

### Environment Variables
- **Security**: All sensitive data in .env file (never committed)
- **Loading**: Use python-dotenv for local development
- **GitHub**: Stored in repository secrets for automation
- **Validation**: Check for required variables at startup

### API Interactions
- **Rate Limiting**: Respect API limits (1.5s between Kalshi requests)
- **Error Handling**: Retry logic with exponential backoff
- **Timeouts**: 30-second timeout for all API calls
- **User Agents**: Descriptive user agent strings for identification

### Data Handling
- **CSV Format**: pandas DataFrame → CSV with headers
- **Timestamps**: ISO format with timezone for collection_date
- **Column Names**: lowercase with underscores (Dune requirement)
- **Schema**: Comprehensive definitions matching all CSV columns