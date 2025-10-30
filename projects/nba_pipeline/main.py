import logging
from datetime import datetime
from src.scraper import NBAPlayerStatsScraper
from src.transformer import NBADataTransformer
from src.loader import NBADataLoader

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_pipeline(season_year=2025):
    """
    Run the complete ETL pipeline.
    
    Args:
        season_year: NBA season ending year (e.g., 2025 for 2024-25 season)
    """
    logger.info("="*80)
    logger.info(f"Starting NBA Data Pipeline for {season_year-1}-{season_year} season")
    logger.info("="*80)
    
    start_time = datetime.now()
    
    try:
        # EXTRACT
        logger.info("\n--- EXTRACT PHASE ---")
        scraper = NBAPlayerStatsScraper()
        raw_data = scraper.scrape_season_totals(season_year)
        
        if not raw_data:
            logger.error("No data extracted. Pipeline failed.")
            return False
        
        logger.info(f"✓ Extracted {len(raw_data)} player records")
        
        # TRANSFORM
        logger.info("\n--- TRANSFORM PHASE ---")
        transformer = NBADataTransformer()
        clean_data = transformer.transform(raw_data)
        
        if clean_data.empty:
            logger.error("Transformation resulted in empty dataset. Pipeline failed.")
            return False
        
        logger.info(f"✓ Transformed data: {clean_data.shape[0]} rows, {clean_data.shape[1]} columns")
        
        # LOAD
        logger.info("\n--- LOAD PHASE ---")
        loader = NBADataLoader()
        
        # Save to CSV
        csv_path = loader.load_to_csv(clean_data, season_year)
        
        # Save to SQLite
        db_success = loader.load_to_sqlite(clean_data, table_name='player_totals', if_exists='replace')
        
        if csv_path and db_success:
            logger.info("✓ Data successfully loaded to both CSV and SQLite")
        
        # SUMMARY
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "="*80)
        logger.info("PIPELINE SUMMARY")
        logger.info("="*80)
        logger.info(f"Status: SUCCESS")
        logger.info(f"Records processed: {len(clean_data)}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"CSV output: {csv_path}")
        logger.info(f"Database: {loader.db_path}")
        logger.info("="*80)
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed with error: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = run_pipeline(2025)
    
    if success:
        print("\n🎉 Pipeline completed successfully!")
    else:
        print("\n❌ Pipeline failed. Check logs above.")