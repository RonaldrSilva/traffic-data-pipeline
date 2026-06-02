import pandas as pd
import pyodbc
import os
import re
import glob
from pathlib import Path
from dotenv import load_dotenv

# setup
SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=SCRIPT_DIR / ".env")

SQL_SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USERNAME = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")

BASE_PATH = SCRIPT_DIR.parent / "data_lake" / "raw"
CONN_STR = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;"

def normalize_columns(name):
    if pd.isna(name): return 'unnamed_column'
    name = str(name).strip().lower()
    name = re.sub(r'[áàâã]', 'a', name)
    name = re.sub(r'[éèê]', 'e', name)
    name = re.sub(r'[íìî]', 'i', name)
    name = re.sub(r'[óòôõ]', 'o', name)
    name = re.sub(r'[úùû]', 'u', name)
    name = name.replace('ç', 'c')
    name = re.sub(r'\s+', '_', name)
    return re.sub(r'[^a-z0-9_]', '', name)

def get_connection():
    return pyodbc.connect(CONN_STR, timeout=120)

def backup_tables(cursor, tables):
    print("Running backup routine...")
    for table in tables:
        bkp_table = f"{table}_bkp"
        try:
            if cursor.execute(f"SELECT OBJECT_ID('{table}', 'U')").fetchone()[0]:
                cursor.execute(f"IF OBJECT_ID('{bkp_table}', 'U') IS NOT NULL DROP TABLE {bkp_table}")
                cursor.execute(f"SELECT * INTO {bkp_table} FROM {table}")
        except Exception as e:
            print(f"Backup failed for {table}: {e}")

def load_data():
    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()

    tables = ["incidents_data", "vehicles_data", "victims_data"]
    backup_tables(cursor, tables)
    conn.commit()

    print("Cleaning target tables...")
    for t in tables:
        cursor.execute(f"IF OBJECT_ID('{t}', 'U') IS NOT NULL DROP TABLE {t}")
    conn.commit()

    for table in tables:
        pattern = os.path.join(BASE_PATH, f"*{table.split('_')[0]}*.xlsx")
        files = glob.glob(pattern)
        if not files: continue

        latest_file = max(files, key=os.path.getmtime)
        print(f"Processing {os.path.basename(latest_file)} into {table}...")
        
        df = pd.read_excel(latest_file)
        df.columns = [normalize_columns(c) for c in df.columns]
        
        # simulated schema creation for portfolio
        # cursor.execute(get_schema_for(table)) 
        # conn.commit()
        
        sql_insert = f"INSERT INTO {table} ({','.join(df.columns)}) VALUES ({','.join(['?']*len(df.columns))})"
        data = [tuple(x) for x in df.to_numpy()]

        try:
            cursor.fast_executemany = True
            cursor.executemany(sql_insert, data)
            conn.commit()
            print(f"Fast insert successful for {table}.")
        except Exception as e:
            print(f"Fast batch failed: {e}. Falling back to safe batch...")
            conn.rollback()
            cursor.fast_executemany = False
            
            # fallback logic
            batch_size = 5000
            for i in range(0, len(data), batch_size):
                cursor.executemany(sql_insert, data[i:i+batch_size])
                conn.commit()
            print("Safe batch completed.")

    conn.close()
    print("Database sync complete.")

if __name__ == "__main__":
    load_data()
