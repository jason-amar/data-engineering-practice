# 🏀 NBA Player Stats ETL Pipeline

![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![Status](https://img.shields.io/badge/status-active-success.svg)

Production-grade ETL pipeline that extracts NBA player statistics from Basketball Reference, transforms and validates the data, and loads it into SQLite and CSV formats for analysis.

## 📊 Pipeline Overview
```
Basketball Reference
        ↓
   EXTRACT (Selenium)
        ↓
  TRANSFORM (Pandas)
        ↓
   VALIDATE (Quality Checks)
        ↓
   LOAD (SQLite + CSV)
```

## ✨ Key Features

### Extract
- **Selenium-based scraping** with anti-detection measures
- **Headless browser** for automated execution
- **Robust error handling** with retry logic
- **Respects rate limiting** (3-second delays)

### Transform
- **Handles traded players** - Consolidates multiple team entries (keeps TOT rows)
- **Data type conversion** - Automatic numeric conversion with error handling
- **Calculated metrics** - Points/rebounds/assists per game, True Shooting %
- **Missing value handling** - Smart imputation based on stat type

### Validate
- **Quality checks** - Detects negative values, unrealistic ages, suspicious scoring
- **Percentage validation** - Ensures shooting percentages in valid ranges
- **Duplicate detection** - Identifies and resolves mid-season trades

### Load
- **Dual output** - SQLite database + timestamped CSV files
- **Organized structure** - Separate raw/processed directories
- **Queryable database** - Ready for analysis and dashboards

## 🛠️ Technologies

- **Python 3.11+**
- **Selenium** - Dynamic web scraping
- **BeautifulSoup4** - HTML parsing
- **Pandas** - Data transformation
- **SQLite3** - Data storage
- **Logging** - Pipeline monitoring

## 🚀 Getting Started

### Prerequisites
- Python 3.11 or higher
- Chrome browser (for Selenium)

### Installation
```bash
# Clone the repository
git clone https://github.com/jason-amar/data-engineering-practice.git
cd data-engineering-practice/projects/nba_pipeline

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Usage
```bash
# Run the complete pipeline for 2024-25 season
python main.py

# Output will be in:
# - data/processed/nba_player_totals_2025_YYYYMMDD_HHMMSS.csv
# - data/nba_stats.db (SQLite database)
```

### Running Individual Components
```bash
# Test scraper only
python src/scraper.py

# Test transformer only
python src/transformer.py

# Test loader only
python src/loader.py
```

## 📁 Project Structure
```
nba_pipeline/
├── src/
│   ├── scraper.py          # Web scraping with Selenium
│   ├── transformer.py      # Data cleaning & transformation
│   └── loader.py           # Data storage (CSV + SQLite)
├── data/
│   ├── raw/                # Raw scraped data (not committed)
│   ├── processed/          # Cleaned data (not committed)
│   └── nba_stats.db        # SQLite database (not committed)
├── logs/                   # Pipeline logs (not committed)
├── main.py                 # Pipeline orchestration
├── config.py               # Configuration settings
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

## 📈 Sample Output

### Pipeline Execution
```
================================================================================
Starting NBA Data Pipeline for 2024-2025 season
================================================================================

--- EXTRACT PHASE ---
✓ Extracted 532 player records

--- TRANSFORM PHASE ---
✓ Transformed data: 532 rows, 35 columns

--- LOAD PHASE ---
✓ Data successfully loaded to both CSV and SQLite

================================================================================
PIPELINE SUMMARY
================================================================================
Status: SUCCESS
Records processed: 532
Duration: 12.34 seconds
CSV output: data/processed/nba_player_totals_2025_20241020_143022.csv
Database: data/nba_stats.db
================================================================================
```

### Top Scorers Query
```python
from src.loader import NBADataLoader

loader = NBADataLoader()
query = """
SELECT player, team_id, pts_per_game, trb_per_game, ast_per_game
FROM player_totals
WHERE pts_per_game > 25
ORDER BY pts_per_game DESC
LIMIT 10
"""
results = loader.query_database(query)
print(results)
```

## 🔍 Data Schema

### Player Stats Table (`player_totals`)

| Column | Type | Description |
|--------|------|-------------|
| player | TEXT | Player name |
| team_id | TEXT | Team abbreviation (TOT = traded) |
| pos | TEXT | Position |
| age | INTEGER | Player age |
| g | INTEGER | Games played |
| pts | INTEGER | Total points |
| pts_per_game | FLOAT | Points per game |
| trb_per_game | FLOAT | Rebounds per game |
| ast_per_game | FLOAT | Assists per game |
| ts_pct | FLOAT | True Shooting % |
| ... | ... | 35+ columns total |

## 🎯 Key Technical Decisions

### Why Selenium over Requests?
Basketball Reference uses JavaScript to load tables dynamically. Selenium ensures we get fully rendered HTML.

### Why Keep TOT Rows?
When players are traded mid-season, Basketball Reference shows stats for each team plus a "TOT" (total) row. We keep TOT rows for accurate season-long statistics.

### Why SQLite?
- Lightweight, no server setup needed
- Perfect for local development and testing
- Easy to migrate to PostgreSQL/MySQL for production
- Built-in Python support

### Calculated Metrics
- **True Shooting %**: More accurate than FG% because it accounts for 3-pointers and free throws
- **Per-game stats**: Normalized for fair player comparison regardless of games played

## ⚠️ Known Limitations

- **Rate limiting**: 3-second delay between requests (could be optimized with async)
- **Single season**: Currently processes one season at a time
- **No incremental loads**: Replaces data on each run (could add upsert logic)
- **Chrome dependency**: Requires Chrome browser for Selenium

## 🔮 Future Enhancements

- [ ] Add scheduling with Apache Airflow/Prefect
- [ ] Implement incremental loading (only new data)
- [ ] Add support for advanced stats (PER, Win Shares, BPM)
- [ ] Create Tableau/Power BI dashboard
- [ ] Add unit tests with pytest
- [ ] Deploy to cloud (Azure Functions/AWS Lambda)
- [ ] Add data quality monitoring/alerting
- [ ] Support multi-season historical loads
- [ ] Add player photo/bio scraping
- [ ] Create REST API wrapper

## 📚 Lessons Learned

**Web Scraping:**
- Basketball Reference's structure is well-organized with `data-stat` attributes
- Selenium is essential for JavaScript-rendered content
- Always implement delays to respect website servers

**Data Quality:**
- Mid-season trades create duplicate player entries - must handle carefully
- Missing values in percentage columns often mean 0 attempts (0/0)
- Always validate numeric ranges to catch scraping errors

**Pipeline Design:**
- Modular ETL components make testing and debugging much easier
- Comprehensive logging saved hours of troubleshooting
- Dual output (CSV + DB) provides flexibility for different use cases

## 🔗 Related Learning

This project applies concepts from:
- [Week 1: Python Foundations](../../week1-setup) - File I/O, logging, error handling
- [Week 1: API Data Extraction](../../week1-setup) - Web scraping techniques
- [Week 1: Data Validation](../../week1-setup) - Quality checks
- [Week 2: SQL & Databases](../../week2-sql) - Database design and queries

## 📞 Questions?

This pipeline was built as part of my data engineering portfolio. Feel free to reach out with questions or suggestions!

---

*Last updated: October 2025*