# Urban Mobility & Traffic Data Pipeline

An automated, resilient Data Engineering pipeline designed to extract, transform, load (ETL), and enrich urban mobility data, traffic incidents, demographics, and vehicle fleet statistics.

## Tech Stack
Language: Python (Pandas, NumPy, scikit-learn, Selenium)
Orchestration: Prefect
Database: SQL Server (ODBC, SQLAlchemy)
Key Techniques: Idempotent Data Loads, Resilient Bulk Ingestion (`fast_executemany` with row-by-row fallback), Web Scraping, Spatial Analysis (`BallTree`).

---

## Pipeline Architecture

The ETL workflow is fully automated and orchestrated via Prefect (`prefect_orchestrator.py`), managing dependencies across three main data domains:

### 1. Traffic Incidents (Core Pipeline)
Extraction (`traffic_incidents_downloader.py`): Automates the download of massive public datasets (.zip/.csv) with retry mechanisms and SSL handling.
Staging Load (`traffic_db_loader.py`): Performs resilient bulk inserts into SQL Server. If dirty data breaks a batch insert, the engine gracefully degrades to a safe row-by-row execution to isolate and log errors without failing the pipeline.
Enrichment (`traffic_enrichment.py`): Processes incremental delta loads, merging new records with historical data and mapping complex road structures.
Data Warehouse Sync (`urban_mobility_loader.py`): Executes idempotent loads into the final analytical tables, ensuring strict referential integrity (Foreign Keys) and preventing duplication.

### 2. Demographics (API Integration)
API Consumer (`demographics_pipeline.py`): Connects to the National Institute of Geography and Statistics (IBGE) API to fetch historical and estimated population data, synchronizing it directly with dimensional tables.

### 3. National Fleet (Web Scraping)
Headless Crawler (`fleet_data_crawler.py`): Uses Selenium in headless mode to navigate government portals, dynamically locate the latest monthly vehicle fleet reports, and download the raw files.
Database Sync (`fleet_db_loader.py`): Cleans, normalizes encodings, and loads the fleet dimensional data into the SQL Server.
