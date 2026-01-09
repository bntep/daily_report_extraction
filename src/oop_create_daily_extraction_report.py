"""
Author: Bertrand, 2025-10-06
This script creates a daily report which shows the daily extraction of stocks database or a monthly report which shows the number of instrument per month in each market exchange
Usage: oop_create_daily_extraction_report.py -p <'daily_extraction', 'monthly_analyze_place_vdf'>
Example: oop_create_daily_extraction_report.py -p daily_extraction
Example: oop_create_daily_extraction_report.py -p monthly_analyze_place_vdf
"""

import os
from pathlib import Path
import sys
import logging
import datetime
import threading
import pandas as pd
import argparse


# rajouter dans la variable d'environnement PATH contenant la liste des répertoires systèmes (programme python, librairies, ...)
# c'est très important quand on crée un package, de rajouter ce répertoire dans PATH
sys.path.append(str(Path(os.getcwd())))
from utils.LogWriter import log_location, log_args, log_configuration
from utils.Toolbox_lib import create_year_calendar
from utils.dbclient.DatabaseClient import DbConnector
from module.env import *

# Path Definitions
HOME = Path(__file__).parent.parent


CHEMIN_RESULTAT = Path(HOME, "resultat/daily_extraction")
CHEMIN_RESULTAT.mkdir(parents=True, exist_ok=True)
LOG_SUBJECT_PREFIX = "LOG_DEV_DAILY_EXTRACTION"
receiver = 'bertrand.ntep@eurofidai.org'
email_message = "Bonjour,\n\nci-joint le rapport journalier de téléchargement des bases de données sur les 15 derniers jours.\n\nCordialement,\n\n\n"

db_logger = log_configuration(log_path=log_location())

def parse_arguments():    
    global CHEMIN_RESULTAT, LOG_SUBJECT_PREFIX
    parser = argparse.ArgumentParser(description="This program creates a report of stocks database extraction.")
    parser.add_argument('--program', '-p', nargs='*',
                        help="choose the type of report: daily_extraction or monthly_analyze_place_vdf", required=True)
    
    parser.add_argument('--database', '-d', nargs='*',
                        help="choose the database: actions, indices, fonds", required=False)
    args = parser.parse_args()

    if args.program[0] not in ['daily_extraction', 'monthly_analyze_place_vdf']:
        raise ValueError("Argument --program must be 'daily_extraction' or 'monthly_analyze_place_vdf'")
    
    if args.program[0] == 'daily_extraction':         
        CHEMIN_RESULTAT = Path(HOME, "resultat/daily_extraction")
        CHEMIN_RESULTAT.mkdir(parents=True, exist_ok=True)
        LOG_SUBJECT_PREFIX = "LOG_DEV_DAILY_EXTRACTION"
    elif args.program[0] == 'monthly_analyze_place_vdf':
        CHEMIN_RESULTAT = Path(HOME, "resultat/analyze_market_exchange")
        CHEMIN_RESULTAT.mkdir(parents=True, exist_ok=True)
        LOG_SUBJECT_PREFIX = "LOG_DEV_MONTHLY_ANALYZE_PLACE_VDF"
    
    if args.database:
        if args.database[0] not in ['actions', 'indices', 'fonds']:
            raise ValueError("Argument --database must be 'actions', 'indices' or 'fonds'")
        
    return args


class BaseInstrument():
    """
    Class defining the instrument to analyse
    """
    
    # reference DataFrames: ALLID_VDF, REF_CODE_PLACE_VDF, REF_CODE_PAYS_VDF
    DF_REF_CODE_PLACE_VDF = DbConnector('durango', echo=True).execute_query(
            "select code_place_vdf, domicile, nom_long from ref_code_place_vdf") 
    DF_REF_ALL_COUNTRY_VDF= DbConnector('durango', echo=True).execute_query(f"select code_vdf_num_pays, \
                                                                         libelle_code_vdf_num_pays_en from ref_code_pays_vdf")
    DF_REF_ALLID_VDF = DbConnector('durango', echo=True).execute_query(
            f"select distinct code_valoren, instrument_type from allid_vdf")          

    def __init__(self, database_name: str) -> None:

        if database_name not in dict_instrument.keys():
            raise ValueError(
                "Nom de base de données incorrect: <actions|indices|fonds>")
        else:
            self.database_name = database_name

            if self.database_name == "actions":
                self.conditions_where  = f" where a.identifiant in (select distinct code_valoren from allid_vdf where instrument_type = {dict_instrument[self.database_name]}) and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null or a.cours_ouverture_official is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null or a.cours_cloture_mid is not null ) or (a.cours_cloture_bid is not null and a.cours_cloture_ask is not null) or (a.best_bid is not null and a.best_ask is not null)) "
            elif self.database_name == "indices":
                # self.conditions_where  = f" where a.identifiant in (select distinct code_valoren from allid_vdf where instrument_type = {dict_instrument[self.database_name]}) \
                #     and (a.cours_cloture_trade is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null or a.cours_ouverture_trade is not null) \
                #         and a.date_cotation >='{DATE_DEBUT_MONTH}' and a.date_cotation <='{DATE_FIN_MONTH}'"
                self.conditions_where  = f" where a.identifiant in (select distinct code_valoren from allid_vdf where instrument_type = {dict_instrument[self.database_name]}) \
                    and (a.cours_cloture_trade is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null or a.cours_ouverture_trade is not null)"
            elif self.database_name == "fonds":
                self.conditions_where  = f"where a.identifiant  in (select distinct code_valoren from allid_vdf where instrument_type = {dict_instrument[self.database_name]}) and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null or a.cours_ouverture_official is not null or a.cours_cloture_mid is not null or a.cours_plus_haut_trade is not null or a.cours_plus_bas_trade is not null) or (a.cours_cloture_bid is not null and a.cours_cloture_ask is not null) or (a.best_bid is not null and a.best_ask is not null) or (a.cours_plus_haut_nav is not null or a.cours_plus_bas_nav is not null) or (a.valeur_fonds_issue is not null) or (valeur_fonds_rp is not null) or (reference_nav is not null) or (provider_assets is not null) or (provider_oustanding_shares is not null))"
            


    def _get_database(self, database_name:str, continent:str=""):
        """
        Factory method to get the appropriate database object based on the database name."""
        if database_name =="actions":
            return Stocks(continent=continent)
        elif database_name =="indices":
            return Indices(continent=continent)
        elif database_name =="fonds":
            return Funds(continent=continent)
        else:
            raise ValueError("Nom de base de données incorrect: <actions|indices|fonds>")
       

    def merge_table(self, df_ref_code_pays_vdf: pd.DataFrame, df_ref_allid_vdf: pd.DataFrame) ->  pd.DataFrame:
    
        """
        Merge the dataframes to create the final report."""
        df_calendrier = create_year_calendar(DATE_DEBUT, DATE_FIN, freq='C')

        df = pd.merge(df_calendrier, dict_df[self.database_name], how='left', on=[
            'date_cotation'])
        df = pd.merge(df, BaseInstrument.DF_REF_CODE_PLACE_VDF, how='left', left_on=[
            'place'], right_on=['code_place_vdf'])
        df = pd.merge(df, df_ref_code_pays_vdf, how='left', left_on=[
            'domicile'], right_on=['code_vdf_num_pays'])
        df = pd.merge(df, df_ref_allid_vdf, how='left', left_on=[
            'identifiant'], right_on=['code_valoren'])
        df = df[df['instrument_type'] == dict_instrument[f"{self.database_name}"]]
        df = df.pivot_table(index=['libelle_code_vdf_num_pays_en', 'place', 'nom_long'], columns=[
            'date_cotation'], values='identifiant', aggfunc=lambda x: len(x.unique()))
        return df
        

    def run_report_database(self) -> None:
        """
        Generate the report for the specified database.
        """
       
        # Filtrer pour obtenir les instruments pertinents
        instrument_type_filter = BaseInstrument.DF_REF_ALLID_VDF["instrument_type"] == dict_instrument[self.database_name]
        df_ref_allid_instr = BaseInstrument.DF_REF_ALLID_VDF[instrument_type_filter]

        # Si le filtre ne retourne qu'une seule ligne, pandas la convertit en Series. On la reconvertit en DataFrame.
        if isinstance(df_ref_allid_instr, pd.Series):
            df_ref_allid_instr = df_ref_allid_instr.to_frame().T

        union_parts = []
        continents = ["europe", "amerique", "asie", "afrique"]
        
        for continent in continents:                
                bdd = self._get_database(self.database_name, continent)                
                union_parts.append(bdd.req_cours                 
                )

        full_req = " UNION ".join(union_parts)
        dict_df[self.database_name] = DbConnector('durango', echo=True).execute_query(full_req)
        dict_df[self.database_name].drop_duplicates(inplace=True)
        df_report = self.merge_table(BaseInstrument.DF_REF_ALL_COUNTRY_VDF, df_ref_allid_instr)

        if self.database_name == "actions":
            # Dans le cas actions on va générer un fichier par continent
            for continent in continents:
                db_logger.info(f"\nStart Processing continent: {continent} for database: {self.database_name}")
                print(f"\nStart Processing continent: {continent} for database {self.database_name}")
                file_to_save = f"{self.database_name}_{continent}_{date}.csv"
                csv_path = Path(CHEMIN_RESULTAT, file_to_save)              
                bdd = self._get_database(self.database_name, continent)                
                df_report_continent = df_report[df_report.index.get_level_values('libelle_code_vdf_num_pays_en').isin(bdd.pays.strip("()").replace("'", "").split(","))]
                df_report_continent.to_csv(csv_path, sep="|", header=True, index=True, encoding='utf-8')
                db_logger.info("CSV File created successfully...%s", csv_path)        
                print("CSV File created successfully... %s", csv_path)
                
        else:
            # Cas pour indices et fonds
            file_to_save = f"{self.database_name}_{date}.csv"
            csv_path = Path(CHEMIN_RESULTAT, file_to_save)           
            df_report.to_csv(csv_path, sep="|", header=True, index=True, encoding='utf-8')
            db_logger.info("CSV File created successfully...%s", csv_path)        
            print("CSV File created successfully... %s", csv_path)
    

    def analyze_market_exchange(self, CHEMIN_RESULTAT):              

        union_parts = []
        for annee in [f"{DATE_YEAR}", f"{DATE_YEAR_N1}"]:
        #for annee in [2020, 2021, 2022, 2023, 2024, 2025]:
            for continent in ['europe', 'asie', 'amerique', 'afrique']:
            #for continent in ['afrique', 'amerique']:
                bdd = self._get_database(self.database_name, continent)
                req_extrac_cours= f"select distinct extract(year from date_cotation) as year, extract(month from date_cotation) as month, identifiant, place from (select {LIST_VAR} from {bdd.table_cours}_{continent}_{annee} as a  {self.conditions_where} ) as t"
                #req_extrac_cours= f"select distinct extract(year from date_cotation) as year, extract(month from date_cotation) as month, identifiant, place from (select {LIST_VAR} from {bdd.table_cours}_{annee} as a  {self.conditions_where} ) as t"
                
                if self.database_name == "indices":
                    bdd_ind_stock= self._get_database("actions", continent)
                    conditions_where_ind_stocks = f" where a.identifiant in (select distinct code_valoren from allid_vdf where instrument_type = {dict_instrument['indices']}) \
                    and ((a.cours_ouverture_trade is not null or a.cours_cloture_trade is not null  or a.cours_plus_haut_trade is not null \
                        or a.cours_plus_bas_trade is not null or a.cours_cloture_mid is not null ) or (a.cours_cloture_bid is not null \
                            and a.cours_cloture_ask is not null) or (a.best_bid is not null and a.best_ask is not null))"
                    req_extrac_ind_stock = f"select distinct extract(year from date_cotation) as year, extract(month from date_cotation) as month, identifiant, place from (select {LIST_VAR} from {bdd_ind_stock.table_cours}_{continent}_{annee} as a  {conditions_where_ind_stocks}) as t"
                    #req_extrac_ind_stock = f"select distinct extract(year from date_cotation) as year, extract(month from date_cotation) as month, identifiant, place from (select {LIST_VAR} from {bdd_ind_stock.table_cours}_{annee} as a  {conditions_where_ind_stocks}) as t"
                    
                    union_parts.append(req_extrac_ind_stock)
                union_parts.append(req_extrac_cours)              
        full_req = " UNION ALL ".join(union_parts)
        #print(full_req)
        df = DbConnector('durango', echo=True).execute_query(full_req)
        #print(df.head())

        # df = pd.concat(frames)        
        df = df.groupby(['year', 'month', 'place']).identifiant.nunique(
        ).reset_index(name='nb_instrument')
        FILE_CONTINENT = f"analyze_market_exchange_{self.database_name}_{date}.csv"
        # df = df.pivot_table(index=['place'], columns=['year','month'], values='identifiant', aggfunc=lambda x: len(x.unique()))
        df = pd.merge(df, BaseInstrument.DF_REF_CODE_PLACE_VDF, how='left',
                      left_on=['place'], right_on=['code_place_vdf'])
        df = pd.merge(df, BaseInstrument.DF_REF_ALL_COUNTRY_VDF, how='left', left_on=[
                      'domicile'], right_on=['code_vdf_num_pays'])
        df = df.pivot_table(index=['libelle_code_vdf_num_pays_en', 'place', 'nom_long'], columns=[
                            'year', 'month'], values='nb_instrument')
        # df = df.pivot_table(index=['libelle_code_vdf_num_pays_en','place', 'nom_long'], columns=['year','month'], values='identifiant', aggfunc=lambda x: len(x.unique()))
        df.to_csv(Path(CHEMIN_RESULTAT, FILE_CONTINENT), sep="|", index=True, encoding='utf-8', header=True)
        db_logger.info('CSV File created successfully... %s', FILE_CONTINENT)
     

class Stocks(BaseInstrument):
    """
    Class defining a Stock instrument whith a specified continent
    """
    def __init__(self, continent: str) -> None:

        BaseInstrument.__init__(self, "actions")

        if continent in ["europe", "amerique", "afrique", "asie"]:
                    self.continent = continent                
                    #self.table_cours = f"src_cours_{self.database_name}_{self.continent}"  
                    self.table_cours = f"src_cours_{self.database_name}" 
                    self.req_cours = f"select date_cotation, identifiant, place from {self.table_cours}_{self.continent} LEFT JOIN ref_code_place_vdf on {self.table_cours}_{self.continent}.place = ref_code_place_vdf.code_place_vdf where ref_code_place_vdf.domicile IN (select code_vdf_num_pays from ref_code_pays_vdf where libelle_code_vdf_num_pays_en in {dict_pays_continent[self.continent]}) AND date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}'"
            
        else:
             raise ValueError("Nom de continent incorrect: <europe|asie|amerique|afrique>")

        #self.req_cours = f"select date_cotation, identifiant, place from {self.table_cours}_{DATE_YEAR} LEFT JOIN ref_code_place_vdf on {self.table_cours}_{DATE_YEAR}.place = ref_code_place_vdf.code_place_vdf where ref_code_place_vdf.domicile IN (select code_vdf_num_pays from ref_code_pays_vdf where libelle_code_vdf_num_pays_en in {dict_pays_continent[self.continent]}) AND date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}'"
        #self.req_cours = f"select date_cotation, identifiant, place from {self.table_cours} LEFT JOIN ref_code_place_vdf on {self.table_cours}.place = ref_code_place_vdf.code_place_vdf where ref_code_place_vdf.domicile IN (select code_vdf_num_pays from ref_code_pays_vdf where libelle_code_vdf_num_pays_en in {dict_pays_continent[self.continent]}) AND date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}'"
        self.database_name_continent = f"{self.database_name}_{self.continent}"
        self.pays = dict_pays_continent[self.continent]  
           

class Indices(BaseInstrument):
    """
    Class defining an Index instrument whith a specified continent
    """
    def __init__(self, continent: str = "") -> None:
        
        BaseInstrument.__init__(self, "indices")

        if continent in ["europe", "amerique", "afrique", "asie"]:
                    self.continent = continent                
                    self.table_cours = f"src_cours_{self.database_name}" 
                    self.req_cours = f"select date_cotation, identifiant, place from {self.table_cours}_{self.continent}_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}'"
               
        else:
             raise ValueError("Nom de continent incorrect: <europe|asie|amerique|afrique>")
        
        #self.req_cours = f"select date_cotation, identifiant, place from {self.table_cours}_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}'"
        self.database_name_continent = f"{self.database_name}_{self.continent}"
        self.pays = ""       


class Funds(BaseInstrument):
    """
    Class defining a Fund instrument whith a specified continent
    """
    def __init__(self, continent: str = "") -> None:

        BaseInstrument.__init__(self, "fonds")

        if continent in ["europe", "amerique", "afrique", "asie"]:
                    self.continent = continent                
                    self.table_cours = f"src_cours_{self.database_name}" 
                    self.req_cours = f"select date_cotation, identifiant, place from {self.table_cours}_{self.continent}_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}'"
              
        else:
             raise ValueError("Nom de continent incorrect: <europe|asie|amerique|afrique>")
        
        #self.req_cours = f"select date_cotation, identifiant, place from {self.table_cours}_{DATE_YEAR} where date_cotation >='{DATE_DEBUT}' and date_cotation<='{DATE_FIN}'"
        self.database_name_continent = f"{self.database_name}_{self.continent}"
        self.pays = ""          


class MyThread(threading.Thread):
    """
    Custom Thread Class
    """

    def __init__(self, name, program):
        threading.Thread.__init__(self)        
        self.name = name        
        self.program = program
        self.exc = None
        
    # Function that raises the custom exception
    def run_thread_daily_report(self, type):        
        bdd = BaseInstrument(type)
        db_logger.info(f"\nStart processing database: {bdd.database_name}\n")
        print(f"\nStart processing database: {bdd.database_name}\n")
        bdd.run_report_database() 
        db_logger.info(f"End of processing all databases...")

    def run_thread_analyze_market_exchange(self, type):        
        bdd = BaseInstrument(type)
        db_logger.info(f"\nStart processing database: {bdd.database_name}\n")
        print(f"\nStart processing database: {bdd.database_name}\n")
        bdd.analyze_market_exchange(CHEMIN_RESULTAT) 
        db_logger.info(f"End of processing all databases...")  

    def run(self):
        # Variable that stores the exception, if raised by someFunction
        if self.program == "daily_extraction":
            try:
                self.run_thread_daily_report(self.name)
            except BaseException as e:
                self.exc = e
        elif self.program == "monthly_analyze_place_vdf":
            try:
                self.run_thread_analyze_market_exchange(self.name)
            except BaseException as e:
                self.exc = e
        else:
            # This case should be caught by argparse, but it's good practice for robustness
            self.exc = ValueError(f"Program argument incorrect: '{self.program}'")

    def join(self):
        threading.Thread.join(self)        
        if self.exc:
            raise ValueError(f"ERROR occurs in the thread {self.name} : {self.exc}")


def run_threads(program: str):

    threads = []

    for thread_index in range(3):       
        thread = MyThread(list(dict_instrument.keys())[thread_index], program=program)
        threads.append(thread)
        thread.start()

    for thread in threads:        
        try:
            thread.join()
        except Exception as e:
            print("ERROR: Exception Handled in Main, Details of the Exception:", e)

    print("All threads have finished.") 


args = parse_arguments()

@log_args(receiver, results_path=CHEMIN_RESULTAT, mail=True, message_email=email_message, started_at=datetime.datetime.now(), hide_args_in_logs=True, subject_prefix=LOG_SUBJECT_PREFIX)
def main():

    db_logger.info(f"\n=================================Start of processing program: {args.program[0]}===================================\n")
    print(f"\n=================================Start of processing program: {args.program[0]}===================================\n")
    
    if args.database:
        print(f"\nDatabase to process: {args.database[0]}\n")
        base = args.database[0]            
        bdd = BaseInstrument(base)
        db_logger.info(f"\nStart processing database: {bdd.database_name}\n")
        print(f"\nStart processing database: {bdd.database_name}\n")
        if args.program[0] == "monthly_analyze_place_vdf":
            bdd.analyze_market_exchange(CHEMIN_RESULTAT)
        elif args.program[0] == "daily_extraction":
            bdd.run_report_database()
    else:
        run_threads(program=args.program[0])    
      

    db_logger.info(f"End of processing all databases...")
    print(f"===================================End of processing all databases.===================================\n")


if __name__ == "__main__":    
    start_time=datetime.datetime.now() 
    db_logger.info(f"command lines called:")
    db_logger.info(f"python3 {__file__}")
    main()
    end_time=datetime.datetime.now()
    print(f"\nStart time: {start_time}\nEnd time: {end_time}\nDuration: {end_time - start_time}")
