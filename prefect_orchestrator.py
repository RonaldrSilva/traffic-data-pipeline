import os
import sys
import subprocess
from datetime import datetime
from prefect import flow, task, get_run_logger

# ==============================================================================
# 1. CORE EXECUTION TASK
# ==============================================================================
@task(name="Execute Python Script", retries=2, retry_delay_seconds=60)
def run_script(file_name):
    logger = get_run_logger()
    logger.info(f"Starting execution: {file_name}")
    
    result = subprocess.run(
        [sys.executable, "-X", "utf8", file_name],
        capture_output=True, 
        text=True,
        encoding='utf-8',
        errors='replace',
        cwd=os.path.dirname(os.path.abspath(__file__)) 
    )
    
    if result.returncode == 0:
        logger.info(f"Successfully finished: {file_name}")
        return result.stdout
    else:
        raise Exception(f"Error in {file_name}:\n{result.stderr}")

# ==============================================================================
# 2. STATE MANAGEMENT TASKS
# ==============================================================================
@task(name="Check Source Status")
def is_already_processed(source_name):
    current_month = datetime.now().strftime("%Y-%m")
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f".status_{source_name}.lock")
    
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            if f.read().strip() == current_month:
                return True
    return False

@task(name="Register Source Success")
def register_success(source_name):
    current_month = datetime.now().strftime("%Y-%m")
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f".status_{source_name}.lock")
    with open(file_path, 'w') as f:
        f.write(current_month)

# ==============================================================================
# 3. MAIN ETL ORCHESTRATION FLOW
# ==============================================================================
@flow(name="Urban Mobility Monthly ETL", timeout_seconds=3600)
def run_monthly_pipeline():
    logger = get_run_logger()
    logger.info("Initializing main orchestrator flow.")

    # ---------------------------------------------------------
    # BLOCK 1: TRAFFIC INCIDENTS (INFOSIGA & CET)
    # ---------------------------------------------------------
    if not is_already_processed("incidents"):
        logger.info("Fetching raw incident data...")
        run_script("traffic_incidents_downloader.py")
        
        logger.info("Loading raw data into staging database...")
        output_db = run_script("traffic_db_loader.py")

        if "Database sync complete" in output_db or "successful" in output_db:
            logger.info("New data ingested. Running enrichment and spatial pipeline...")
            run_script("traffic_enrichment.py")
            
            logger.info("Syncing enriched data to final target tables...")
            run_script("cet_data_loader.py") 
            
            register_success("incidents")
        else:
            logger.info("No new incident data available yet. Will retry next run.")
    else:
        logger.info("Incidents pipeline already completed for this month.")

    # ---------------------------------------------------------
    # BLOCK 2: DEMOGRAPHICS (IBGE)
    # ---------------------------------------------------------
    if not is_already_processed("demographics"):
        logger.info("Fetching demographics data from external API...")
        output_demo = run_script("demographics_pipeline.py")
        
        if "target table updated" in output_demo:
            logger.info("Demographics data successfully synced.")
            register_success("demographics")
        else:
            logger.info("No new demographics data. Will retry next run.")
    else:
        logger.info("Demographics already completed for this month.")

    # ---------------------------------------------------------
    # BLOCK 3: FLEET DATA (SENATRAN)
    # ---------------------------------------------------------
    if not is_already_processed("fleet"):
        logger.info("Crawling government portal for new fleet data...")
        output_fleet = run_script("fleet_data_crawler.py")
        
        if "Fleet data updated" in output_fleet:
            logger.info("New fleet data downloaded. Syncing to database...")
            run_script("fleet_db_loader.py")
            register_success("fleet")
        else:
            logger.info("Source portal has not released new data yet.")
    else:
        logger.info("Fleet data already completed for this month.")

# ==============================================================================
# 4. SCHEDULER ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    today = datetime.now()
    if 14 <= today.day <= 22:
        run_monthly_pipeline()
    else:
        print(f"Current day is {today.day}. Orchestrator is scheduled to run between the 14th and 22nd.")
