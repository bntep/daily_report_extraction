"""
Author: Bertrand, 2024-06-04
This script creates a daily report of stocks database extraction.
Usage: create_daily_extraction_report.py
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
from utils.LogWriter import log_location, log_config, log_args
from utils.Toolbox_lib import create_year_calendar
from utils.dbclient.DatabaseClient import DbConnector
from module.env import *

# Path Definitions
HOME = Path(__file__).parent.parent
CHEMIN_RESULTAT = Path(HOME, "resultat/daily_extraction")
CHEMIN_RESULTAT.mkdir(parents=True, exist_ok=True)
receiver = 'bertrand.ntep@eurofidai.org'
email_message = "Bonjour,\n\nci-joint le rapport journalier de téléchargement des bases de données sur les 15 derniers jours.\n\nCordialement,\n\n\n"

# Custom Thread Class
class MyThread(threading.Thread):

    def __init__(self, name):
        threading.Thread.__init__(self)
        # self.threadID = threadID
        self.name = name
        # self.counter = counter

    # Function that raises the custom exception

    def execute_create_daily_report(self, type):
        base = BaseInstrument(type)
        db_logger.info(f"début du programme ... {type}")
        base.create_report(CHEMIN_RESULTAT)
        db_logger.info(f"Fin du programme ... {type}")
    

    def run(self):

        # Variable that stores the exception, if raised by someFunction
        self.exc = None
        try:
            self.execute_create_daily_report(self.name)
        except BaseException as e:
            self.exc = e

    def join(self):
        threading.Thread.join(self)
        # Since join() returns in caller thread
        # we re-raise the caught exception
        # if any was caught
        if self.exc:
            raise ValueError(f"ERROR occurs in the thread {self.name} : {self.exc}")
       


class BaseInstrument():

    def __init__(self, basename: str, continent=None):
        self.basename = basename
        self.continent = continent
        self.table_cours = dict_table_cours[self.basename]

        if self.basename not in dict_table_cours.keys():
            raise ValueError("Nom de base inconnu: <actions|indices|fonds>")

        if self.continent is not None:
            if self.continent in ["europe", "amerique", "afrique", "asie"]:
                self.table_cours = f"cours_{self.basename}_{self.continent}"
            else:
                raise ValueError(
                    "Nom de continent incorrect: <europe|asie|amerique|afrique>")

        if self.basename == "actions":
            globals()[f"self.req_{self.basename}"] = f"select date_cotation, identifiant, place from (select * from {self.table_cours}_europe_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}' UNION select * from  {self.table_cours}_asie_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}' UNION select * from  {self.table_cours}_amerique_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}' UNION  select * from  {self.table_cours}_afrique_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}') as a where identifiant is not null and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null or a.cours_ouverture_official is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null or a.cours_cloture_mid is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null) or (a.cours_cloture_bid is not null and a.cours_cloture_ask is not null) or (a.best_bid is not null and a.best_ask is not null));"

        elif self.basename == "indices":
            globals()[f"self.req_{self.basename}"] = f"select date_cotation, identifiant, place from (select * from {self.table_cours}_europe_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}' UNION select * from  {self.table_cours}_asie_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}' UNION select * from  {self.table_cours}_amerique_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}' UNION  select * from  {self.table_cours}_afrique_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}') as a where identifiant is not null and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null or a.cours_cloture_mid is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null) or (a.cours_cloture_bid is not null and a.cours_cloture_ask is not null));"

        elif self.basename == "fonds":
            globals()[f"self.req_{self.basename}"] = f"select date_cotation, identifiant, place from (select * from {self.table_cours}_europe_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}' UNION select * from  {self.table_cours}_asie_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}' UNION select * from  {self.table_cours}_amerique_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}' UNION  select * from  {self.table_cours}_afrique_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}') as a where identifiant is not null and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null or a.cours_ouverture_official is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null or a.cours_cloture_mid is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null) or (a.cours_cloture_bid is not null and a.cours_cloture_ask is not null) or (a.best_bid is not null and a.best_ask is not null) or (a.cours_plus_haut_nav is not null or a.cours_plus_bas_nav is not null) or (a.valeur_fonds_issue is not null) or (valeur_fonds_rp is not null) or (reference_nav is not null) or (provider_assets is not null) or (provider_oustanding_shares is not null));"

    def create_report(self, CHEMIN_RESULTAT):
        """ Connect to the PostgreSQL database server """
        df_ref_code_place_vdf = DbConnector('durango', echo=True).execute_query(
            "select code_place_vdf, domicile, nom_long from ref_code_place_vdf")

        df_ref_allid_vdf = DbConnector('durango', echo=True).execute_query(
            "select code_valoren, instrument_type from allid_vdf")

        if self.basename == "actions":            

            dict_df[self.basename] = DbConnector('durango', echo=True).execute_query(
                globals()[f"self.req_{self.basename}"])         
            
            for continent in dict_pays_continent.keys():

                FILE_CONTINENT = f"{self.basename}_{continent}_{date}.csv"

                # Connect to the PostgreSQL database server
                req_ref_code_pays_vdf = f"select code_vdf_num_pays, libelle_code_vdf_num_pays_en from ref_code_pays_vdf where libelle_code_vdf_num_pays_en in {dict_pays_continent[continent]}"

                df_ref_code_pays_vdf = DbConnector(
                'durango', echo=True).execute_query(req_ref_code_pays_vdf)
                # join variables from different tables
                merge_table(self.basename, df_ref_code_pays_vdf,
                            df_ref_code_place_vdf, df_ref_allid_vdf, FILE_CONTINENT)

        else:

            FILE_CONTINENT = f"{self.basename}_{date}.csv"            

            dict_df[self.basename] = DbConnector('durango', echo=True).execute_query(
                globals()[f"self.req_{self.basename}"])

            df_ref_code_pays_vdf = DbConnector('durango', echo=True).execute_query(
                f"select code_vdf_num_pays, libelle_code_vdf_num_pays_en from ref_code_pays_vdf")

            # join variables from different tables"""
            merge_table(self.basename, df_ref_code_pays_vdf,
                        df_ref_code_place_vdf, df_ref_allid_vdf, FILE_CONTINENT)

    
def merge_table(type_instrument: str, df_ref_code_pays_vdf: pd.DataFrame, df_ref_code_place_vdf: pd.DataFrame, df_ref_allid_vdf: pd.DataFrame, file_to_send: str):
    
    # Create year Calendar Dataframe
    df_calendrier = create_year_calendar(DATE_DEBUT, DATE_FIN, freq='C')

    df = pd.merge(df_calendrier, dict_df[type_instrument], how='left', on=[
        'date_cotation'])
    df = pd.merge(df, df_ref_code_place_vdf, how='left', left_on=[
        'place'], right_on=['code_place_vdf'])
    df = pd.merge(df, df_ref_code_pays_vdf, how='left', left_on=[
        'domicile'], right_on=['code_vdf_num_pays'])
    df = pd.merge(df, df_ref_allid_vdf, how='left', left_on=[
        'identifiant'], right_on=['code_valoren'])
    df = df[df['instrument_type'] ==
            dict_instrument[f"{type_instrument}"]]
    df = df.pivot_table(index=['libelle_code_vdf_num_pays_en', 'place', 'nom_long'], columns=[
        'date_cotation'], values='identifiant', aggfunc=lambda x: len(x.unique()))
    df.to_csv(Path(CHEMIN_RESULTAT, file_to_send))
    db_logger.info(
        'CSV File created successfully... %s', file_to_send)


def log_config(log_path: Path, name=__name__) -> object:
    logging.basicConfig(
        filename=log_path,
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s -%(levelname)s - %(message)s',
        datefmt='%Y%m%d %H:%M:%S',
        filemode='a',
    )

    logging.getLogger('sqlalchemy.engine')
    db_logger = logging.getLogger(name)

    return db_logger


db_logger = log_config(log_path=log_location())


def run_threads():

    threads = []

    for thread_index in range(3):
        # thread = threading.Thread(target=execute_create_daily_report, args=(list(dict_instrument.keys())[thread_index],))
        thread = MyThread(list(dict_instrument.keys())[thread_index])
        threads.append(thread)
        thread.start()

    for thread in threads:
        # thread.join()
        try:
            thread.join()
        except Exception as e:
            print("ERROR: Exception Handled in Main, Details of the Exception:", e)

    print("All threads have finished.")


@log_args(receiver, results_path=CHEMIN_RESULTAT, mail=True, message_email=email_message, started_at=datetime.datetime.now(), hide_args_in_logs=True, subject_prefix="LOG_DEV")
def main():
    run_threads()
    

db_logger.info(f"command lines called:")
db_logger.info(f"python3 {__file__}")
main()


# if __name__ == "__main__":    
#     db_logger.info(f"command lines called:")
#     db_logger.info(f"python3 {__file__}")
#     main()
