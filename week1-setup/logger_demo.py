"""
Demonstration of Python logging for data engineering
Author: Jason Amar
Date: 2025-10-20
"""

import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Capture all log levels
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),  # Write to file
        logging.StreamHandler()  # Also print to console
    ]
)

# Create logger
logger = logging.getLogger(__name__)

def simulate_data_pipeline():
    """
    Simulates a simple data pipeline with logging at each step
    """
    logger.info("="*50)
    logger.info("Starting data pipeline")
    logger.info(f"Pipeline started at: {datetime.now()}")
    
    try:
        # Step 1: Extract
        logger.info("Step 1: Extracting data from source")
        records_extracted = 1000
        logger.info(f"Successfully extracted {records_extracted} records")
        
        # Step 2: Transform
        logger.info("Step 2: Transforming data")
        logger.debug("Applying data cleaning rules...")
        logger.debug("Removing duplicates...")
        records_after_transform = 950
        logger.warning(f"Removed {records_extracted - records_after_transform} duplicate records")
        
        # Step 3: Validate
        logger.info("Step 3: Validating data quality")
        null_count = 5
        if null_count > 0:
            logger.warning(f"Found {null_count} null values in critical columns")
        
        # Step 4: Load
        logger.info("Step 4: Loading data to destination")
        logger.info(f"Successfully loaded {records_after_transform} records")
        
        logger.info("="*50)
        logger.info("Pipeline completed successfully!")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        logger.exception("Full traceback:")  # This logs the full error stack
        return False

def demonstrate_log_levels():
    """
    Shows different logging levels and when to use them
    """
    logger.debug("DEBUG: Detailed diagnostic info (use during development)")
    logger.info("INFO: General informational messages (normal operations)")
    logger.warning("WARNING: Something unexpected but not breaking (missing data, deprecated feature)")
    logger.error("ERROR: Serious problem, function failed (but app continues)")
    logger.critical("CRITICAL: System failure, app may crash")

if __name__ == "__main__":
    print("\n--- Demonstrating Log Levels ---\n")
    demonstrate_log_levels()
    
    print("\n--- Simulating Data Pipeline ---\n")
    success = simulate_data_pipeline()
    
    if success:
        print("\n✅ Check 'app.log' file to see the complete log output!")
    else:
        print("\n❌ Pipeline failed - check logs for details")