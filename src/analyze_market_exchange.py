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


parser = argparse.ArgumentParser(description="This program analyzes the appearances and disappearances of stock exchange market. Param: a List of instrument to analyze.\
                         The list must contains actions or indices or fonds")

parser.add_argument('--type_instrument', '-t', nargs='*',
                    help="a List of instrument to analyze. The type of instrument: actions, indices, fonds", required=True)

args = parser.parse_args()


# Path and Variables
HOME = Path(__file__).parent.parent
CHEMIN_RESULTAT = Path(HOME, "resultat/analyze_market_exchange")
CHEMIN_RESULTAT.mkdir(parents=True, exist_ok=True)
receiver = 'bertrand.ntep@eurofidai.org'
email_message = "Bonjour,\n\nci-joint l'analyse des places vdf reçues en téléchargement sur les 12 derniers mois.\n\nCordialement,\n\n\n"

db_logger = log_configuration(log_path=log_location())

# Custom Thread Class
class MyThread(threading.Thread):

    def __init__(self, name, file_path):
        threading.Thread.__init__(self)
        # self.threadID = threadID
        self.name = name
        self.path = file_path

    # Function that raises the custom exception

    def execute_market_exchange(self,type):
        base = BaseInstrument(type)
        db_logger.info(f"début du traitement de la base ... {type}")
        base.analyze_market_exchange(self.path)
        db_logger.info(f"Fin du traitement de la base ... {type}")
    

    def run(self):
        # Variable that stores the exception, if raised by someFunction
        self.exc = None
        try:
            self.execute_market_exchange(self.name)
        except BaseException as e:
            self.exc = e

    def join(self):
        threading.Thread.join(self)
        # Since join() returns in caller thread, we re-raise the caught exception       
        if self.exc:
            raise ValueError(f"ERROR occurs in the thread {self.name} : {self.exc}")
            db_logger.error(f"ERROR: An ERROR occurs in the thread {self.name} : {self.exc}")

       

class BaseInstrument():

    """
    Class defining the instrument to analyse
    """

    def __init__(self, basename: str, continent=None):
        self.basename = basename
        self.continent = continent
        self.table_cours = dict_table_cours[self.basename]

        if self.basename not in dict_table_cours.keys():
            raise ValueError(
                "Nom de base inconnu: <actions|indices|fonds>")

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

   
    def analyze_market_exchange(self, CHEMIN_RESULTAT):

        dict_df = {'actions': [], 'indices': [], 'fonds': []}

        where_conditions_actions = f" where a.identifiant in (select distinct code_valoren from allid_vdf where instrument_type = {dict_instrument[self.basename]}) and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null or a.cours_ouverture_official is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null or a.cours_cloture_mid is not null ) or (a.cours_cloture_bid is not null and a.cours_cloture_ask is not null) or (a.best_bid is not null and a.best_ask is not null)) and a.date_cotation >='{DATE_DEBUT_MONTH}' and a.date_cotation <='{DATE_FIN_MONTH}'"

        where_conditions_ind_actions = f"where a.identifiant  in (select distinct code_valoren from allid_vdf where instrument_type = 7) and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null or a.cours_ouverture_official is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null or a.cours_cloture_mid is not null ) or (a.cours_cloture_bid is not null and a.cours_cloture_ask is not null) or (a.best_bid is not null and a.best_ask is not null)) and a.date_cotation >='{DATE_DEBUT_MONTH}' and a.date_cotation <='{DATE_FIN_MONTH}'"

        where_conditions_indices = f" where a.identifiant in (select distinct code_valoren from allid_vdf where instrument_type = {dict_instrument[self.basename]}) and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null  or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null or a.cours_cloture_mid is not null ) or (a.cours_cloture_bid is not null and a.cours_cloture_ask is not null) or (a.best_bid is not null and a.best_ask is not null)) and a.date_cotation >='{DATE_DEBUT_MONTH}' and a.date_cotation <='{DATE_FIN_MONTH}'"

        where_conditions_fonds = f"where a.identifiant  in (select distinct code_valoren from allid_vdf where instrument_type = {dict_instrument[self.basename]}) and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null or a.cours_ouverture_official is not null or a.cours_cloture_mid is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null) or (a.cours_cloture_bid is not null and a.cours_cloture_ask is not null) or (a.best_bid is not null and a.best_ask is not null) or (a.cours_plus_haut_nav is not null or a.cours_plus_bas_nav is not null) or (a.valeur_fonds_issue is not null) or (valeur_fonds_rp is not null) or (reference_nav is not null) or (provider_assets is not null) or (provider_oustanding_shares is not null)) and a.date_cotation >='{DATE_DEBUT_MONTH}' and a.date_cotation <='{DATE_FIN_MONTH}'"

        dict_conditions = {'actions': where_conditions_actions, 'indices': where_conditions_indices,
                           'fonds': where_conditions_fonds, 'ind_actions': where_conditions_ind_actions}

        """ Connect to the PostgreSQL database server """
        df_ref_code_place_vdf = DbConnector('durango', echo=True).execute_query(
            "select code_place_vdf, domicile, nom_long from ref_code_place_vdf")

        df_ref_code_pays_vdf = DbConnector('durango', echo=True).execute_query(
            "select code_vdf_num_pays, libelle_code_vdf_num_pays_en from ref_code_pays_vdf")

        for annee in [f"{DATE_YEAR}", f"{DATE_YEAR_N1}"]:
            for continent in ['europe', 'asie', 'amerique', 'afrique']:
                globals()[f"req_{self.basename}_{continent}_{annee}"] = f"select distinct extract(year from date_cotation) as year, extract(month from date_cotation) as month, identifiant, place from (select {LIST_VAR} from {self.table_cours}_{continent}_{annee} as a  {dict_conditions[self.basename]}) as t;"
                dict_req[f"df_{self.basename}_{continent}_{annee}"] = globals(
                )[f"req_{self.basename}_{continent}_{annee}"]
                dict_df[self.basename].append(
                    f"df_{self.basename}_{continent}_{annee}")

                if self.basename == 'indices':
                    globals()[f"reqind_actions_{continent}_{annee}"] = f"select distinct extract(year from date_cotation) as year, extract(month from date_cotation) as month, identifiant, place from (select {LIST_VAR} from src_cours_actions_{continent}_{annee} as a  {dict_conditions['ind_actions']}) as t;"
                    dict_req[f"dfind_actions_{continent}_{annee}"] = globals()[
                        f"reqind_actions_{continent}_{annee}"]
                    dict_df[self.basename].append(
                        f"dfind_actions_{continent}_{annee}")

        frames = []
        """ Connect to the PostgreSQL database server """
        for tab in dict_df[self.basename]:
            print(dict_req[tab])
            res = DbConnector(
                'durango', echo=True).execute_query(dict_req[tab])
            frames.append(res)

        df = pd.concat(frames)
        # df = df.groupby(['year','month','place']).count().reset_index(names='counts')
        # df = df.groupby(['year','month','place']).size().reset_index(name ='nb_instrument')
        df = df.groupby(['year', 'month', 'place']).identifiant.nunique(
        ).reset_index(name='nb_instrument')
        FILE_CONTINENT = f"analyze_market_exchange_{self.basename}_{date}.csv"
        # df = df.pivot_table(index=['place'], columns=['year','month'], values='identifiant', aggfunc=lambda x: len(x.unique()))
        df = pd.merge(df, df_ref_code_place_vdf, how='left',
                      left_on=['place'], right_on=['code_place_vdf'])
        df = pd.merge(df, df_ref_code_pays_vdf, how='left', left_on=[
                      'domicile'], right_on=['code_vdf_num_pays'])
        df = df.pivot_table(index=['libelle_code_vdf_num_pays_en', 'place', 'nom_long'], columns=[
                            'year', 'month'], values='nb_instrument')
        # df = df.pivot_table(index=['libelle_code_vdf_num_pays_en','place', 'nom_long'], columns=['year','month'], values='identifiant', aggfunc=lambda x: len(x.unique()))
        df.to_csv(Path(CHEMIN_RESULTAT, FILE_CONTINENT), sep="|", index=True, encoding='utf-8', header=True)
        db_logger.info('CSV File created successfully... %s', FILE_CONTINENT)


# def log_configuration(log_path: Path, name=__name__) -> object:
#     logging.basicConfig(
#         filename=log_path,
#         level=logging.DEBUG,
#         format='%(asctime)s - %(name)s -%(levelname)s - %(message)s',
#         datefmt='%Y%m%d %H:%M:%S',
#         filemode='a',
#     )

#     logging.getLogger('sqlalchemy.engine')
#     db_logger = logging.getLogger(name)

#     return db_logger


# db_logger = log_configuration(log_path=log_location())


# def merge_table(type_instrument: str):
#     df = pd.merge(df_calendrier, dict_df[type_instrument], how='left', on=[
#         'date_cotation'])
#     df = pd.merge(df, df_ref_code_place_vdf, how='left', left_on=[
#         'place'], right_on=['code_place_vdf'])
#     df = pd.merge(df, df_ref_code_pays_vdf, how='left', left_on=[
#         'domicile'], right_on=['code_vdf_num_pays'])
#     df = pd.merge(df, df_ref_allid_vdf, how='left', left_on=[
#         'identifiant'], right_on=['code_valoren'])
#     df = df[df['instrument_type'] ==
#             dict_instrument[f"{type_instrument}"]]
#     df = df.pivot_table(index=['libelle_code_vdf_num_pays_en', 'place', 'nom_long'], columns=[
#         'date_cotation'], values='identifiant', aggfunc=lambda x: len(x.unique()))
#     # df_actions_continent= df_actions_continent.applymap(remove_non_printable_chars)
#     df.to_csv(Path(CHEMIN_RESULTAT, FILE_CONTINENT))
#     db_logger.info(
#         'CSV File created successfully... %s', FILE_CONTINENT)

def run_threads():

    threads = []

    for thread_index in range(len(args.type_instrument)):
        # thread = threading.Thread(target=execute_create_daily_report, args=(list(dict_instrument.keys())[thread_index],))
        thread = MyThread(args.type_instrument[thread_index], CHEMIN_RESULTAT)
        threads.append(thread)
        thread.start()

    for thread in threads:        
        try:
            thread.join()
        except Exception as e:
            print("ERROR: Exception Handled in Main, Details of the Exception:", e)
            db_logger.error(f"ERROR: Exception Handled in Main, Details of the Exception: {e}")


    print("All threads have finished.")


@log_args(receiver, results_path=CHEMIN_RESULTAT, mail=True, message_email=email_message, started_at=datetime.datetime.now(), hide_args_in_logs=True, subject_prefix="LOG_DEV")
def main():

    if isInclude(['actions', 'indices', 'fonds'], args.type_instrument):
        run_threads()

    else:
        db_logger.info(f"le paramètre {args.type_instrument} n'est pas valide.")
        raise ValueError(
            f"le paramètre {args.type_instrument} n'est pas valide.")
        # sys.exit(1)
    db_logger.info(
        f"le traitement de la base {args.type_instrument} est terminé. \n le fichier est disponible dans le répertoire {CHEMIN_RESULTAT} \n")
    


db_logger.info(f"command lines called:")
db_logger.info(f"python3 {__file__}")
main()


# if __name__ == "__main__":
#     db_logger.info(f"command lines called:")
#     db_logger.info(f"python3 {__file__}")
#     main()
