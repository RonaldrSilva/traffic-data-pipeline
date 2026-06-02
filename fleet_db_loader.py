import pandas as pd
from sqlalchemy import create_engine, Date 
from pathlib import Path
import urllib.parse
import os
from dotenv import load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=SCRIPT_DIR / ".env")

SQL_SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USERNAME = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")

SOURCE_FILE = SCRIPT_DIR.parent / "data_lake" / "fleet" / "national_fleet_history.xlsx"
TABLE_NAME = 'dim_fleet_history'

def load_fleet_to_db():
    if not SOURCE_FILE.exists():
        print("Source file missing.")
        return

    df = pd.read_excel(SOURCE_FILE)
    if 'date_ref' in df.columns:
        df['date_ref'] = pd.to_datetime(df['date_ref'], dayfirst=True, errors='coerce').dt.date
    
    params = urllib.parse.quote_plus(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;")
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
    
    print(f"Syncing {len(df)} records to target database...")
    df.to_sql(TABLE_NAME, con=engine, if_exists='replace', index=False, dtype={'date_ref': Date()})
    print("Database sync complete.")

if __name__ == "__main__":
    load_fleet_to_db()
