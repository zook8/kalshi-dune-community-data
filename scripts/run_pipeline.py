#!/usr/bin/env python3
"""
Main Pipeline Runner - Orchestrates daily Kalshi data collection and Dune upload
"""

import sys
import subprocess
from pathlib import Path
import logging
from datetime import datetime

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent

# Ensure logs directory exists
(PROJECT_ROOT / "logs").mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_script(script_path, script_name):
    """Run a Python script and return success status"""
    try:
        logger.info(f"Starting {script_name}...")
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        if result.returncode == 0:
            logger.info(f"{script_name} completed successfully")
            if result.stdout:
                logger.info(f"{script_name} output: {result.stdout}")
            return True
        else:
            logger.error(f"{script_name} failed with return code {result.returncode}")
            if result.stderr:
                logger.error(f"{script_name} error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"{script_name} timed out after 10 minutes")
        return False
    except Exception as e:
        logger.error(f"Failed to run {script_name}: {e}")
        return False

def main():
    """Run the complete pipeline"""
    logger.info("=" * 60)
    logger.info("STARTING KALSHI → DUNE PIPELINE")
    logger.info("=" * 60)
    
    scripts_dir = PROJECT_ROOT / "scripts"
    
    # Step 1: Collect Kalshi data
    collect_success = run_script(scripts_dir / "kalshi_collector.py", "Kalshi Data Collection")
    
    if not collect_success:
        logger.error("Data collection failed. Stopping pipeline.")
        return False
    
    # Step 2: Upload to Dune
    upload_success = run_script(scripts_dir / "dune_uploader.py", "Dune Upload")
    
    if not upload_success:
        logger.error("Dune upload failed. Data collected but not uploaded.")
        return False
    
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
