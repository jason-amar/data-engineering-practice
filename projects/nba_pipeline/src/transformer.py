import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NBADataTransformer:
    """
    Transforms raw scraped NBA data into clean, analysis-ready format.
    """
    
    def __init__(self):
        # Column name mapping (Basketball Reference names -> Standard abbreviations)
        self.column_mapping = {
            'games': 'g',
            'games_started': 'gs',
            'team_name_abbr': 'team_id'
        }

        # Define which columns should be numeric
        self.numeric_columns = [
            'age', 'g', 'gs', 'mp',  # Basic stats
            'fg', 'fga', 'fg_pct',  # Field goals
            'fg3', 'fg3a', 'fg3_pct',  # Three pointers
            'fg2', 'fg2a', 'fg2_pct',  # Two pointers
            'efg_pct',  # Effective FG%
            'ft', 'fta', 'ft_pct',  # Free throws
            'orb', 'drb', 'trb',  # Rebounds
            'ast', 'stl', 'blk', 'tov', 'pf',  # Other stats
            'pts'  # Points
        ]
        
    def transform(self, raw_data):
        """
        Main transformation pipeline.
        
        Args:
            raw_data: List of dictionaries from scraper
        
        Returns:
            pandas DataFrame with cleaned data
        """
        if not raw_data:
            logger.warning("No data to transform")
            return pd.DataFrame()
        
        logger.info(f"Starting transformation of {len(raw_data)} records")

        # Convert to DataFrame
        df = pd.DataFrame(raw_data)

        logger.info(f"Initial shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")

        # Step 0: Standardize column names
        df = self._standardize_columns(df)

        # Step 1: Handle duplicate players (players traded mid-season)
        df = self._handle_duplicates(df)
        
        # Step 2: Convert data types
        df = self._convert_data_types(df)
        
        # Step 3: Handle missing values
        df = self._handle_missing_values(df)
        
        # Step 4: Add calculated fields
        df = self._add_calculated_fields(df)
        
        # Step 5: Data quality checks
        self._quality_checks(df)
        
        logger.info(f"Transformation complete. Final shape: {df.shape}")
        
        return df

    def _standardize_columns(self, df):
        """
        Rename columns to standard abbreviations for consistency.
        """
        logger.info("Standardizing column names...")

        # Rename columns based on mapping
        df = df.rename(columns=self.column_mapping)

        logger.info(f"Columns after standardization: {df.columns.tolist()}")

        return df

    def _handle_duplicates(self, df):
        """
        Handle players who appear multiple times (traded during season).
        Basketball Reference shows separate rows for each team plus a 'TOT' (total) row.
        """
        logger.info("Handling duplicate players...")
        
        initial_count = len(df)
        
        # Check if we have duplicate players
        duplicates = df[df.duplicated(subset=['player'], keep=False)]
        
        if len(duplicates) > 0:
            logger.info(f"Found {len(duplicates)} duplicate player entries (traded players)")
            
            # Keep only 'TOT' rows for traded players, or first occurrence if no TOT
            def keep_row(group):
                # If there's a TOT row, keep only that
                tot_rows = group[group['team_id'] == 'TOT']
                if len(tot_rows) > 0:
                    return tot_rows
                # Otherwise keep the first occurrence
                return group.head(1)
            
            df = df.groupby('player', as_index=False).apply(keep_row).reset_index(drop=True)
        
        final_count = len(df)
        logger.info(f"Removed {initial_count - final_count} duplicate rows")
        
        return df
    
    def _convert_data_types(self, df):
        """
        Convert string columns to appropriate numeric types.
        """
        logger.info("Converting data types...")
        
        for col in self.numeric_columns:
            if col in df.columns:
                # Convert to numeric, coercing errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Season year should be integer
        if 'season_year' in df.columns:
            df['season_year'] = pd.to_numeric(df['season_year'], errors='coerce')
        
        logger.info("Data type conversion complete")
        
        return df
    
    def _handle_missing_values(self, df):
        """
        Handle missing/null values appropriately.
        """
        logger.info("Handling missing values...")
        
        # Log missing value counts
        missing_counts = df[self.numeric_columns].isnull().sum()
        if missing_counts.sum() > 0:
            logger.info(f"Missing values found:\n{missing_counts[missing_counts > 0]}")
        
        # For percentage columns, NaN often means 0 attempts (0/0 = undefined)
        # We'll keep as NaN rather than filling with 0
        
        # For counting stats (games, points, etc.), NaN likely means 0
        counting_stats = ['g', 'gs', 'fg', 'fga', 'fg3', 'fg3a', 'fg2', 'fg2a',
                         'ft', 'fta', 'orb', 'drb', 'trb', 'ast', 'stl', 'blk', 
                         'tov', 'pf', 'pts', 'mp']
        
        for col in counting_stats:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        return df
    
    def _add_calculated_fields(self, df):
        """
        Add useful calculated fields.
        """
        logger.info("Adding calculated fields...")
        
        # Points per game
        if 'pts' in df.columns and 'g' in df.columns:
            df['pts_per_game'] = df['pts'] / df['g'].replace(0, 1)  # Avoid division by zero
            df['pts_per_game'] = df['pts_per_game'].round(2)
        
        # Rebounds per game
        if 'trb' in df.columns and 'g' in df.columns:
            df['trb_per_game'] = df['trb'] / df['g'].replace(0, 1)
            df['trb_per_game'] = df['trb_per_game'].round(2)
        
        # Assists per game
        if 'ast' in df.columns and 'g' in df.columns:
            df['ast_per_game'] = df['ast'] / df['g'].replace(0, 1)
            df['ast_per_game'] = df['ast_per_game'].round(2)
        
        # Minutes per game
        if 'mp' in df.columns and 'g' in df.columns:
            df['mp_per_game'] = df['mp'] / df['g'].replace(0, 1)
            df['mp_per_game'] = df['mp_per_game'].round(1)
        
        # True Shooting Percentage (advanced stat)
        # TS% = PTS / (2 * (FGA + 0.44 * FTA))
        if all(col in df.columns for col in ['pts', 'fga', 'fta']):
            denominator = 2 * (df['fga'] + 0.44 * df['fta'])
            df['ts_pct'] = (df['pts'] / denominator.replace(0, 1)) * 100
            df['ts_pct'] = df['ts_pct'].round(1)
        
        logger.info(f"Added {5} calculated fields")
        
        return df
    
    def _quality_checks(self, df):
        """
        Perform data quality checks and log warnings.
        """
        logger.info("Performing quality checks...")
        
        # Check for negative values (shouldn't happen in basketball stats)
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                logger.warning(f"Found {negative_count} negative values in {col}")
        
        # Check for unrealistic values
        if 'age' in df.columns:
            unrealistic_ages = df[(df['age'] < 18) | (df['age'] > 50)]
            if len(unrealistic_ages) > 0:
                logger.warning(f"Found {len(unrealistic_ages)} players with unrealistic ages")
        
        if 'pts_per_game' in df.columns:
            high_scorers = df[df['pts_per_game'] > 50]
            if len(high_scorers) > 0:
                logger.warning(f"Found {len(high_scorers)} players averaging >50 PPG (possible data issue)")
        
        # Check percentage ranges (should be 0-100 or 0-1 depending on format)
        pct_cols = [col for col in df.columns if 'pct' in col]
        for col in pct_cols:
            if col in df.columns:
                out_of_range = df[(df[col] < 0) | (df[col] > 100)].dropna()
                if len(out_of_range) > 0:
                    logger.warning(f"Found {len(out_of_range)} out-of-range values in {col}")
        
        logger.info("Quality checks complete")


# Test function
if __name__ == "__main__":
    # Import scraper to test end-to-end
    import sys
    sys.path.append('..')
    from src.scraper import NBAPlayerStatsScraper
    
    print("\n" + "="*80)
    print("Testing Transformation Pipeline")
    print("="*80 + "\n")
    
    # Scrape data
    scraper = NBAPlayerStatsScraper()
    raw_data = scraper.scrape_season_totals(2025)
    
    if raw_data:
        print(f"Scraped {len(raw_data)} players\n")
        
        # Transform data
        transformer = NBADataTransformer()
        clean_data = transformer.transform(raw_data)
        
        print("\n" + "="*80)
        print("Transformation Results")
        print("="*80)
        print(f"\nDataFrame shape: {clean_data.shape}")
        print(f"\nData types:\n{clean_data.dtypes}")
        print(f"\nFirst 3 players:\n{clean_data.head(3)}")
        print(f"\nBasic statistics:\n{clean_data[['pts_per_game', 'trb_per_game', 'ast_per_game']].describe()}")
        
        # Show top 5 scorers
        print("\n" + "="*80)
        print("Top 5 Scorers (PPG)")
        print("="*80)
        top_scorers = clean_data.nlargest(5, 'pts_per_game')[['player', 'team_id', 'pts_per_game', 'g']]
        print(top_scorers.to_string(index=False))
    else:
        print("Failed to scrape data")