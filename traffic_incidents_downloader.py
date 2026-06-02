import pandas as pd
import requests
import zipfile
import os
import shutil
import time
from datetime import datetime 
from pathlib import Path
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fix_coordinates(df):
    if df is None or df.empty: return df
    target_cols = [c for c in df.columns if 'latitude' in c.lower() or 'longitude' in c.lower()]
    for col in target_cols:
        df[col] = df[col].astype(str).str.replace(',', '.')
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def clean_raw_data(df):
    if df is None or df.empty: return df
    qty_cols = [c for c in df.columns if c.startswith('qtd_')]
    for col in qty_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    text_cols = df.select_dtypes(include=['object']).columns
    for col in text_cols:
        df[col] = df[col].replace({'S': 'SIM', 's': 'SIM', 'Sim': 'SIM', 'sim': 'SIM'})
    return df

def fetch_and_process_data():
    print(f"Starting data extraction at {datetime.now().strftime('%H:%M:%S')}")
    
    base_path = Path(__file__).resolve().parent.parent / "data_lake" / "raw"
    temp_path = base_path / "temp_extract"
    zip_file = base_path / "traffic_data.zip"

    for p in [base_path, temp_path]:
        p.mkdir(parents=True, exist_ok=True)

    url = "https://infosiga.detran.sp.gov.br/rest/painel/download/file/dados_infosiga.zip"
    
    session = requests.Session()
    retry = Retry(connect=3, read=3, backoff_factor=2)
    session.mount('https://', HTTPAdapter(max_retries=retry))

    print("Downloading source ZIP...")
    with session.get(url, stream=True, verify=False, timeout=180) as r:
        r.raise_for_status()
        with open(zip_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=131072): 
                if chunk: f.write(chunk)

    print("Extracting files...")
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(temp_path)

    def load_and_filter(pattern):
        files = list(temp_path.rglob(f"*{pattern}*.csv"))
        df_list = []
        for f in files:
            df_temp = pd.read_csv(f, sep=';', encoding='ISO-8859-1', low_memory=False)
            df_temp.columns = [c.lower() for c in df_temp.columns]
            df_list.append(df_temp)
        
        if not df_list: return pd.DataFrame()
        df_final = pd.concat(df_list, ignore_index=True)
        return fix_coordinates(clean_raw_data(df_final))

    print("Processing incidents...")
    df_incidents = load_and_filter("sinistros")
    df_incidents = df_incidents[df_incidents['municipio'] == 'SAO PAULO']
    valid_ids = df_incidents['id_sinistro'].unique()

    print("Processing victims and vehicles...")
    df_people = load_and_filter("pessoas")
    df_people = df_people[df_people['id_sinistro'].isin(valid_ids)] if not df_people.empty else df_people
    
    df_vehicles = load_and_filter("veiculos")
    df_vehicles = df_vehicles[df_vehicles['id_sinistro'].isin(valid_ids)] if not df_vehicles.empty else df_vehicles

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    df_incidents.to_excel(base_path / f"incidents_{timestamp}.xlsx", index=False)
    df_people.to_excel(base_path / f"victims_{timestamp}.xlsx", index=False)
    df_vehicles.to_excel(base_path / f"vehicles_{timestamp}.xlsx", index=False)

    shutil.rmtree(temp_path, ignore_errors=True)
    if zip_file.exists(): zip_file.unlink()
    print("Extraction complete.")

if __name__ == "__main__":
    fetch_and_process_data()
