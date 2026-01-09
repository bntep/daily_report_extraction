"""
Author: Bertrand, 2024-06-04
This script creates a monhtly report which shows the number of instrument per month in each market exchange
Usage: analyze_market_exchange.py -t <'actions', 'indices', 'fonds'>
Example: analyze_market_exchange.py -t actions
Example: analyze_market_exchange.py -t actions indices
"""

# from sqlalchemy import create_engine
# from sqlalchemy import text
# from sqlalchemy.engine import URL
import datetime
import os
from pathlib import Path
import sys
import argparse
import pandas as pd
import logging
import threading


# rajouter dans la variable d'environnement PATH contenant la liste des répertoires systèmes (programme python, librairies, ...)
# c'est très important quand on crée un package, de rajouter ce répertoire dans PATH

sys.path.append(str(Path(os.getcwd())))
from utils.LogWriter import log_location, log_args, log_configuration
from utils.dbclient.DatabaseClient import DbConnector
from utils.Toolbox_lib import isInclude, create_year_calendar
from module.env import *
from src.oop_create_daily_extraction_report import BaseInstrument,Stocks, Indices, Funds

# Path and Variables
HOME = Path(__file__).parent.parent
CHEMIN_RESULTAT = Path(HOME, "resultat/analyze_market_exchange")
CHEMIN_RESULTAT.mkdir(parents=True, exist_ok=True)
receiver = 'bertrand.ntep@eurofidai.org'
email_message = "Bonjour,\n\nci-joint l'analyse des places vdf reçues en téléchargement sur les 12 derniers mois.\n\nCordialement,\n\n\n"

db_logger = log_configuration(log_path=log_location())

parser = argparse.ArgumentParser(description="This program analyzes the appearances and disappearances of stock exchange market. Param: a List of instrument to analyze.\
                         The list must contains actions or indices or fonds")

parser.add_argument('--type_instrument', '-t', nargs='*',
                    help="a List of instrument to analyze. The type of instrument: actions, indices, fonds", required=False)

args = parser.parse_args()


if __name__ == "__main__":    
    start_time=datetime.datetime.now() 
    # if args.type_instrument:
    #     list_instrument= args.type_instrument
    #     print(f"List of instrument to analyze: {list_instrument}")        
    #     if not isInclude(list_instrument, ['actions', 'indices', 'fonds']):
    #         print("Error: The type of instrument must be in the list: actions, indices, fonds")
    #         sys.exit(1)
    base = BaseInstrument("fonds")
    db_logger.info(f"début du traitement de la base ...{base.database_name}")
    base.analyze_market_exchange(CHEMIN_RESULTAT)
    db_logger.info(f"Fin du traitement de la base ...{base.database_name}")
    end_time=datetime.datetime.now()
    print(f"Durée totale d'exécution du programme: {end_time - start_time}")



