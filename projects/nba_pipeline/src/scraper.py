from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NBAPlayerStatsScraper:
    """
    Scrapes NBA player season totals from Basketball Reference.
    """
    
    def __init__(self):
        self.base_url = "https://www.basketball-reference.com"
        self.driver = None
        self.request_delay = 3
    
    def _setup_driver(self):
        """
        Set up Chrome driver with options to avoid detection.
        """
        if self.driver is None:
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Run in background
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info("Chrome driver initialized")

    def get_page_content(self, url):
        """
        Fetch HTML content from a URL using Selenium.
        """
        try:
            self._setup_driver()
            logger.info(f"Fetching: {url}")
            self.driver.get(url)

            # Wait for the table to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "totals_stats"))
            )

            time.sleep(self.request_delay)
            return self.driver.page_source
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def close(self):
        """
        Close the browser driver.
        """
        if self.driver:
            self.driver.quit()
            logger.info("Browser closed")
    
    def scrape_season_totals(self, season_year=2025):
        """
        Scrape player season totals for a given season.
        
        Args:
            season_year: The ending year of the season (e.g., 2025 for 2024-25 season)
        
        Returns:
            list of dictionaries containing player stats
        """
        url = f"{self.base_url}/leagues/NBA_{season_year}_totals.html"
        
        html_content = self.get_page_content(url)
        if not html_content:
            return []
        
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Find the stats table
        # Basketball Reference uses id="totals_stats" for this table
        stats_table = soup.find('table', {'id': 'totals_stats'})
        
        if not stats_table:
            logger.error("Could not find stats table on page")
            return []
        
        players = []
        
        # Get table headers to understand column structure
        headers = []
        thead = stats_table.find('thead')
        if thead:
            header_row = thead.find_all('tr')[-1]  # Get the last header row (has actual column names)
            headers = [th.get('data-stat', th.text) for th in header_row.find_all('th')]
        
        logger.info(f"Found {len(headers)} columns: {headers[:10]}...")  # Show first 10
        
        # Get table body
        tbody = stats_table.find('tbody')
        if not tbody:
            logger.error("Could not find table body")
            return []
        
        rows = tbody.find_all('tr', class_=lambda x: x != 'thead')  # Skip header rows in body
        
        logger.info(f"Found {len(rows)} player rows")
        
        for row in rows:
            try:
                # Skip rows that are just headers (Basketball Reference repeats headers)
                if 'thead' in row.get('class', []):
                    continue
                
                player_data = self._parse_player_row(row, headers, season_year)
                if player_data:
                    players.append(player_data)
                    
            except Exception as e:
                logger.error(f"Error parsing row: {e}")
                continue
        
        logger.info(f"Successfully parsed {len(players)} players")
        return players
    
    def _parse_player_row(self, row, headers, season_year):
        """
        Parse a single player row from the stats table.

        Basketball Reference uses data-stat attributes which makes parsing easier!
        """
        player_data = {
            'season_year': season_year,
            'scraped_at': datetime.now().isoformat()
        }

        # Get all cells in the row
        cells = row.find_all(['th', 'td'])

        for cell in cells:
            # Basketball Reference uses 'data-stat' to identify columns
            stat_name = cell.get('data-stat')

            if not stat_name:
                continue

            # Extract text value
            value = cell.text.strip()

            # Handle player name link specially (could be 'player' or 'name_display')
            if stat_name in ['player', 'name_display']:
                link = cell.find('a')
                if link:
                    player_data['player_url'] = self.base_url + link.get('href', '')
                # Store with consistent key name
                player_data['player'] = value

            # Store the value with original column name
            player_data[stat_name] = value

        # Only return if we got a player name (check both possible column names)
        if 'player' in player_data and player_data['player']:
            return player_data

        return None


# Test function
if __name__ == "__main__":
    scraper = NBAPlayerStatsScraper()

    try:
        # Scrape 2024-25 season
        players = scraper.scrape_season_totals(2025)

        if players:
            print(f"\nSuccessfully scraped {len(players)} players!")
            print("\n" + "="*80)
            print("Sample of first 3 players:")
            print("="*80)

            for player in players[:3]:
                print(f"\nPlayer: {player.get('player', 'Unknown')}")
                print(f"  Team: {player.get('team_id', 'N/A')}")
                print(f"  Position: {player.get('pos', 'N/A')}")
                print(f"  Age: {player.get('age', 'N/A')}")
                print(f"  Games: {player.get('g', 'N/A')}")
                print(f"  Points: {player.get('pts', 'N/A')}")
                print(f"  Rebounds: {player.get('trb', 'N/A')}")
                print(f"  Assists: {player.get('ast', 'N/A')}")

            print("\n" + "="*80)
            print(f"All available stats for first player:")
            print("="*80)
            for key, value in players[0].items():
                print(f"  {key}: {value}")
        else:
            print("No players scraped. Check the logs above for errors.")
    finally:
        # Always close the browser
        scraper.close()