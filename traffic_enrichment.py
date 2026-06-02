import pandas as pd
import shutil
import glob
import os
from pathlib import Path

# setup paths
BASE_DIR = Path(__file__).resolve().parent.parent / "data_lake"
RAW_DIR = BASE_DIR / "raw"
PROCESSED_DIR = BASE_DIR / "processed"
MASTER_FILE = PROCESSED_DIR / "master_mobility_data.xlsx"

def apply_complex_roads_mapping(df):
    print("Mapping complex road structures...")
    mapping_file = BASE_DIR / "auxiliary" / "complex_roads_map.xlsx"
    
    if not mapping_file.exists():
        df['complex_road_name'] = None
        df['is_complex'] = 'NO'
        return df

    df_map = pd.read_excel(mapping_file)
    df_map['road_code'] = df_map['road_code'].astype(str).str.replace(r'\.0$', '', regex=True)
    df['temp_code'] = df['codlog'].astype(str).str.replace(r'\.0$', '', regex=True)

    code_dict = df_map.set_index('road_code')['is_complex'].to_dict()
    df['complex_road_name'] = df['temp_code'].map(code_dict)
    
    fallback_col = 'street_name_source' if 'street_name_source' in df.columns else 'logradouro'
    df['complex_road_name'] = df['complex_road_name'].combine_first(df[fallback_col])
    df['is_complex'] = df['temp_code'].map(code_dict).notna().map({True: 'YES', False: 'NO'})
    
    return df.drop(columns=['temp_code'])

def process_incremental_load():
    files = glob.glob(os.path.join(RAW_DIR, "incidents_*.xlsx"))
    if not files:
        print("No raw files found.")
        return

    latest_file = max(files, key=os.path.getmtime)
    df_new = pd.read_excel(latest_file)
    df_new['id_sinistro'] = df_new['id_sinistro'].astype(str)

    processed_ids = set()
    if MASTER_FILE.exists():
        df_master = pd.read_excel(MASTER_FILE, usecols=['id_sinistro'])
        processed_ids = set(df_master['id_sinistro'].astype(str))

    df_delta = df_new[~df_new['id_sinistro'].isin(processed_ids)]
    
    if df_delta.empty:
        print("No new records to process. Master file is up to date.")
        return

    print(f"Processing {len(df_delta)} new records...")
    df_enriched = apply_complex_roads_mapping(df_delta)

    if MASTER_FILE.exists():
        df_master = pd.read_excel(MASTER_FILE)
        df_final = pd.concat([df_master, df_enriched], ignore_index=True)
    else:
        df_final = df_enriched

    temp_path = PROCESSED_DIR / "temp_master.xlsx"
    df_final.to_excel(temp_path, index=False)
    shutil.copy2(temp_path, MASTER_FILE)
    temp_path.unlink()
    
    print("Incremental load finished successfully.")

if __name__ == "__main__":
    process_incremental_load()
