import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NBADataLoader:
    """
    Loads transformed NBA data into storage (CSV and/or SQLite).
    """
    
    def __init__(self, data_dir='data'):
        self.data_dir = data_dir
        self.raw_dir = os.path.join(data_dir, 'raw')
        self.processed_dir = os.path.join(data_dir, 'processed')
        self.db_path = os.path.join(data_dir, 'nba_stats.db')
        
        # Create directories if they don't exist
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
    
    def load_to_csv(self, df, season_year):
        """
        Save DataFrame to CSV file.
        
        Args:
            df: pandas DataFrame
            season_year: Season year for filename
        
        Returns:
            Path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"nba_player_totals_{season_year}_{timestamp}.csv"
        filepath = os.path.join(self.processed_dir, filename)
        
        try:
            df.to_csv(filepath, index=False)
            logger.info(f"Data saved to CSV: {filepath}")
            logger.info(f"Rows saved: {len(df)}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return None
    
    def load_to_sqlite(self, df, table_name='player_totals', if_exists='replace'):
        """
        Save DataFrame to SQLite database.
        
        Args:
            df: pandas DataFrame
            table_name: Name of the table
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Connect to database
            conn = sqlite3.connect(self.db_path)
            
            # Write DataFrame to SQL
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            
            # Get row count to confirm
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            conn.close()
            
            logger.info(f"Data saved to SQLite: {self.db_path}")
            logger.info(f"Table: {table_name}")
            logger.info(f"Rows in table: {row_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving to SQLite: {e}")
            return False
    
    def query_database(self, query):
        """
        Execute a SQL query and return results as DataFrame.
        
        Args:
            query: SQL query string
        
        Returns:
            pandas DataFrame with query results
        """
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            logger.error(f"Error querying database: {e}")
            return pd.DataFrame()


# Test function
if __name__ == "__main__":
    import sys
    sys.path.append('..')
    from src.scraper import NBAPlayerStatsScraper
    from src.transformer import NBADataTransformer
    
    print("\n" + "="*80)
    print("Testing Load Pipeline")
    print("="*80 + "\n")
    
    # Get and transform data
    scraper = NBAPlayerStatsScraper()
    raw_data = scraper.scrape_season_totals(2025)
    
    if raw_data:
        transformer = NBADataTransformer()
        clean_data = transformer.transform(raw_data)
        
        # Load data
        loader = NBADataLoader()
        
        # Save to CSV
        csv_path = loader.load_to_csv(clean_data, 2025)
        
        # Save to SQLite
        success = loader.load_to_sqlite(clean_data)
        
        if success:
            # Test querying the database
            print("\n" + "="*80)
            print("Testing Database Query")
            print("="*80)
            
            query = """
            SELECT player, team_id, pts_per_game, trb_per_game, ast_per_game
            FROM player_totals
            WHERE pts_per_game > 25
            ORDER BY pts_per_game DESC
            LIMIT 10
            """
            
            results = loader.query_database(query)
            print("\nTop scorers (>25 PPG):")
            print(results.to_string(index=False))