"""
Author: Bertrand, 2024-04-01
This script creates a report of green bonds database extraction.
Usage: create_green_bonds_extraction_report.py
"""


import os
from pathlib import Path
import sys
import logging
import datetime
import threading
import pandas as pd


# rajouter dans la variable d'environnement PATH contenant la liste des répertoires systèmes (programme python, librairies, ...)
# c'est très important quand on crée un package, de rajouter ce répertoire dans PATH
sys.path.append(str(Path(os.getcwd())))
from utils.LogWriter import log_location, log_args, log_configuration
from utils.Toolbox_lib import create_year_calendar
from utils.dbclient.DatabaseClient import DbConnector
from module.env import *

# Path Definitions
HOME = Path(__file__).parent.parent
CHEMIN_RESULTAT = Path(HOME, "resultat/green_bonds_extraction")
CHEMIN_RESULTAT.mkdir(parents=True, exist_ok=True)
receiver = 'bertrand.ntep@eurofidai.org'
email_message = "Bonjour,\n\nci-joint le rapport journalier de téléchargement des bases de données sur les 15 derniers jours.\n\nCordialement,\n\n\n"


db_logger = log_configuration(log_path=log_location())


def greenbonds_report():
    """
    This function creates a report of green bonds database extraction.
    It connects to the database, retrieves data, and saves it to a CSV file.
    """
    req_valuation_price = f"select date_extraction, count(*) as nb_lignes, count(distinct isin) as nb_sin, count(distinct swissvalornumber) as nb_valoren, \
    count(distinct bc) as nb_place, (SELECT count(*) from information_schema.columns where table_name='src_flex_cours_greenbonds') as nb_col \
    from src_flex_cours_greenbonds group by date_extraction order by date_extraction desc limit 15"
   
    db_logger.info(f"Executing SQL query for valuation price ...")
        # Connect to the PostgreSQL database server
    df_valuation_price = DbConnector(
                'durango', echo=True).execute_query(req_valuation_price)
    db_logger.info(f"Control metrics for green bonds valuation price ...:\n{df_valuation_price}")
   
    
    
    req_financial_char = f"select date_extraction, count(*) as nb_lignes, count(distinct identifier) as nb_sin, count(distinct swissvalornumber) as nb_valoren, \
    count(distinct bc) as nb_place, (SELECT count(*) from information_schema.columns where table_name='src_greenbonds_dev_data_madeforyou') as nb_col \
    from src_greenbonds_dev_data_madeforyou group by date_extraction order by date_extraction desc limit 15"
   
    db_logger.info(f"Executing SQL query for financial characteristics ...")
        # Connect to the PostgreSQL database server
    df_financial_char = DbConnector(
                'durango', echo=True).execute_query(req_financial_char)
    db_logger.info(f"Control metrics for green bonds Financial Characteristics ...:\n{df_financial_char}")
  
    req_gss_char = f"select date_extraction, count(*) as nb_lignes, count(distinct isin) as nb_sin, \
    (SELECT count(*) from information_schema.columns where table_name='src_greenbonds_dev_data_mainstreet') as nb_col \
    from src_greenbonds_dev_data_mainstreet group by date_extraction order by date_extraction desc limit 15"

    try:
        db_logger.info(f"Executing SQL query for green bonds GSS Characteristics ...")
        # Connect to the PostgreSQL database server
        df_gss_char = DbConnector(
                'durango', echo=True).execute_query(req_gss_char)    
        db_logger.info(f"Control metrics for green bonds GSS Characteristics ...:\n{df_gss_char}")
    except Exception as e:  
        db_logger.error(f"ERROR: An error occurred while executing SQL query for GSS characteristics: {e}")
        raise

@log_args(receiver, results_path=CHEMIN_RESULTAT, mail=True, message_email=email_message, started_at=datetime.datetime.now(), hide_args_in_logs=True, subject_prefix="LOG_DEV_GREENBONDS_EXTRACTION")
def main():
    db_logger.info(f"command lines called:")
    db_logger.info(f"python3 {__file__}\n")
    db_logger.info(f"Current working directory: {os.getcwd()}\n")
    db_logger.info(f"Starting green bonds extraction report...")    
    greenbonds_report()    
    db_logger.info(f"Green bonds extraction report completed.")
    


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    db_logger.info(f"Script started: {datetime.datetime.now()}")
    
    # Run the main function
    try:
        main()
    except Exception as e:
        db_logger.error(f"An error occurred: {e}")
        raise
    finally:
        db_logger.info(f"Script finished: {datetime.datetime.now()}")      
        db_logger.info(f"Log file closed.")
        db_logger.info(f"End of script.")