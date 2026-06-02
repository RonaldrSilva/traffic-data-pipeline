import pandas as pd
import requests
import urllib.parse
from sqlalchemy import create_engine, types
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv

load_dotenv()
SQL_SERVER, DATABASE = os.getenv("DB_SERVER"), os.getenv("DB_NAME")
USERNAME, PASSWORD = os.getenv("DB_USER"), os.getenv("DB_PASS")

def fetch_ibge_data(url, source_name):
    try:
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False).json()
        series = resp[0]['resultados'][0]['series'][0]['serie']
        df = pd.DataFrame(list(series.items()), columns=['year', 'population'])
        df = df[df['population'] != '...']
        df['population'] = df['population'].astype(int)
        df['year'] = df['year'].astype(int)
        df['source'] = source_name
        return df
    except Exception as e:
        print(f"API fetch failed for {source_name}: {e}")
        return pd.DataFrame()

def run_demographics_sync():
    print("Initializing IBGE demographics sync...")
    
    # city code 3550308 = São Paulo
    url_est = "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/all/variaveis/9324?localidades=N6[3550308]"
    url_censo = "https://servicodados.ibge.gov.br/api/v3/agregados/631/periodos/all/variaveis/93?localidades=N6[3550308]"

    df_est = fetch_ibge_data(url_est, "estimativa")
    df_cen = fetch_ibge_data(url_censo, "censo")
    
    df_ibge = pd.concat([df_est, df_cen]).sort_values(by='year').drop_duplicates(subset=['year'], keep='last')
    
    if df_ibge.empty:
        return

    params = urllib.parse.quote_plus(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;")
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    df_ibge.to_sql(
        'dim_population', 
        engine, 
        if_exists='replace', 
        index=False, 
        dtype={'year': types.Integer(), 'population': types.Integer()}
    )
    print("Demographics target table updated.")

if __name__ == "__main__":
    run_demographics_sync()
