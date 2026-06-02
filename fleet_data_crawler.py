import pandas as pd
import requests
import time
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import urllib3

urllib3.disable_warnings()

BASE_DIR = Path(__file__).resolve().parent.parent / "data_lake" / "fleet"
MASTER_FLEET_FILE = BASE_DIR / "national_fleet_history.xlsx"

MONTHS_MAP = {'JANEIRO': 1, 'FEVEREIRO': 2, 'MARCO': 3, 'ABRIL': 4, 'MAIO': 5, 'JUNHO': 6, 
              'JULHO': 7, 'AGOSTO': 8, 'SETEMBRO': 9, 'OUTUBRO': 10, 'NOVEMBRO': 11, 'DEZEMBRO': 12}

def get_fleet_links(driver, year):
    url = f"https://www.gov.br/transportes/pt-br/assuntos/transito/conteudo-Senatran/frota-de-veiculos-{year}"
    driver.get(url)
    time.sleep(2)

    links_dict = {}
    titles = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'frota nacional')]")
    
    for title in titles:
        month_val = next((num for name, num in MONTHS_MAP.items() if name in title.text.upper()), 0)
        if not month_val: continue
            
        try:
            link_element = title.find_element(By.XPATH, "following::a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZÇÍ', 'abcdefghijklmnopqrstuvwxyzci'), 'munic')][1]")
            links_dict[month_val] = link_element.get_attribute('href')
        except:
            pass
            
    return links_dict

def process_fleet_file(url, year, month):
    temp_file = BASE_DIR / f"temp_{year}_{month}.xlsx"
    resp = requests.get(url, verify=False, timeout=60)
    with open(temp_file, 'wb') as f: f.write(resp.content)
        
    df_sp = pd.DataFrame()
    for skip in range(6):
        try:
            df = pd.read_excel(temp_file, skiprows=skip)
            df.columns = [str(c).strip().upper() for c in df.columns]
            if 'UF' in df.columns:
                df_sp = df[(df['UF'].str.strip() == 'SP') & (df['MUNICIPIO'].str.contains('SAO PAULO'))]
                if not df_sp.empty: break 
        except:
            continue
            
    temp_file.unlink(missing_ok=True)
    if df_sp.empty: raise ValueError("Target city not found in file.")
        
    row = df_sp.iloc[0]
    return {
        'date_ref': f"{month:02d}/{year}",
        'year': int(year),
        'month': int(month),
        'total_vehicles': int(row.get('TOTAL', 0))
    }

def run_crawler():
    options = Options()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=options)

    try:
        df_history = pd.read_excel(MASTER_FLEET_FILE) if MASTER_FLEET_FILE.exists() else pd.DataFrame()
        # simplified backfill logic for portfolio representation
        current_year = datetime.now().year
        links = get_fleet_links(driver, current_year)
        
        new_data = []
        for month, link in links.items():
            print(f"Fetching data for {month}/{current_year}...")
            new_data.append(process_fleet_file(link, current_year, month))
            
        if new_data:
            df_history = pd.concat([df_history, pd.DataFrame(new_data)]).drop_duplicates()
            df_history.to_excel(MASTER_FLEET_FILE, index=False)
            print("Fleet data updated.")
            
    finally:
        driver.quit()

if __name__ == "__main__":
    run_crawler()
