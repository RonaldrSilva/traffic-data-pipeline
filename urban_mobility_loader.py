import pandas as pd
import os
import urllib.parse
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv

# setup environment and paths
SCRIPT_DIR = Path(__file__).resolve().parent
ENV_PATH = SCRIPT_DIR / ".env"

if not ENV_PATH.exists():
    raise FileNotFoundError(f"Missing configuration file at: {ENV_PATH}")

load_dotenv(dotenv_path=ENV_PATH)

SQL_SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USERNAME = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASS")

BASE_DIR = SCRIPT_DIR.parent
DATA_DIR = BASE_DIR / "data" / "cet"
INPUT_FILE = DATA_DIR / "urban_mobility_pre_processed.xlsx"

# database engine setup
params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SQL_SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def ensure_target_table_exists():
    sql_create_table = """
    IF OBJECT_ID('dbo.Fact_Urban_Mobility_Processed', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.Fact_Urban_Mobility_Processed (
            id_sinistro INT NOT NULL,
            logradouro NVARCHAR(255),
            street_number NVARCHAR(50),
            latitude_original FLOAT,
            longitude_original FLOAT,
            latitude_geocode FLOAT,
            longitude_geocode FLOAT,
            road_code NVARCHAR(20),
            standardized_street_name NVARCHAR(255),
            traffic_department_id NVARCHAR(10),
            regional_group_id NVARCHAR(10),
            sub_prefecture_id NVARCHAR(10),
            district_name NVARCHAR(100),
            region_name NVARCHAR(50),
            road_classification NVARCHAR(50),
            similarity_score INT,
            distance_km FLOAT,
            outside_boundary VARCHAR(10),
            search_source NVARCHAR(50),
            complex_road_name NVARCHAR(255),
            is_complex_road NVARCHAR(10),

            CONSTRAINT fk_mobility_incidents_parent
                FOREIGN KEY (id_sinistro)
                REFERENCES sinistros_infosiga (id_sinistro)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        );
    END
    """
    with engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT").execute(text(sql_create_table))

def load_and_sync_data():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Pre-processed source file not found at: {INPUT_FILE}")

    print(f"Reading staging data from: {INPUT_FILE.name}")
    df = pd.read_excel(INPUT_FILE, engine='openpyxl')
    df['similarity_score'] = df['similarity_score'].fillna(0).astype(int)

    print("Validating foreign key constraints and existing keys...")
    
    # fetch parent keys to avoid integrity errors
    query_parent_keys = "SELECT DISTINCT id_sinistro FROM dbo.sinistros_infosiga"
    parent_ids = pd.read_sql(query_parent_keys, engine)
    set_parent_ids = set(parent_ids['id_sinistro'])

    # fetch existing destination keys to prevent duplication
    query_target_keys = "SELECT DISTINCT id_sinistro FROM dbo.Fact_Urban_Mobility_Processed"
    target_ids = pd.read_sql(query_target_keys, engine)
    set_target_ids = set(target_ids['id_sinistro'])

    # filtering
    df_valid = df[df['id_sinistro'].isin(set_parent_ids)].copy()
    df_new_records = df_valid[~df_valid['id_sinistro'].isin(set_target_ids)].copy()

    print(f"Extraction summary:")
    print(f"Total rows in source: {len(df)}")
    print(f"Rows ignored (FK violations): {len(df) - len(df_valid)}")
    print(f"Rows ignored (Already in database): {len(df_valid) - len(df_new_records)}")
    print(f"New records to insert: {len(df_new_records)}")

    if not df_new_records.empty:
        target_columns = [
            'id_sinistro', 'logradouro', 'street_number', 'latitude_original', 
            'longitude_original', 'latitude_geocode', 'longitude_geocode', 'road_code', 
            'standardized_street_name', 'traffic_department_id', 'regional_group_id', 'sub_prefecture_id', 
            'district_name', 'region_name', 'road_classification', 'similarity_score', 'distance_km', 
            'outside_boundary', 'search_source', 'complex_road_name', 'is_complex_road'
        ]
        
        final_df = df_new_records[[c for c in target_columns if c in df_new_records.columns]]
        
        try:
            final_df.to_sql(
                'Fact_Urban_Mobility_Processed',
                engine,
                if_exists='append',
                index=False,
                chunksize=500
            )
            print("Data ingestion loop executed successfully.")
        except Exception as e:
            print(f"Database insertion error: {e}")
    else:
        print("Pipeline target table is already synchronized.")

if __name__ == "__main__":
    print("Starting process: cet_data_loader")
    ensure_target_table_exists()
    load_and_sync_data()
