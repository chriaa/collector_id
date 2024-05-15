import logging
import os
import sys
import datetime
from pathlib import Path

from pyspark import rdd

#sys.path.append('../')  # Adjust the path as necessary
sys.path.append(str(Path(__file__).resolve().parent.parent))

from resource_api.OrcidAPI import OrcidAPI, partition_search
from resource_api.OpenAlexAPI import OpenAlexAPI
from database.DBAccess import DBAccess
from utils.CleanData import (CleanData, process_search_name_data, instantiate_error_log, filter_titles, filter_multiple_collectors, filter_institutions, process_search_name_iterator)
from utils.FuzzyMatchNames import FuzzyMatchNames, get_best_match_and_confidence, get_best_match_iterator
from dotenv import load_dotenv


from pyspark.sql import SparkSession
import pandas as pd

# Load the environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



date_time = datetime.datetime.now()

def output_file_rdd(spark, file_name, rdd):
    if not rdd.isEmpty():
        df = spark.createDataFrame(rdd)
        #df.write.csv(path = f"./{file_name}.csv", mode = 'overwrite', header= True)
        df.coalesce(1).write.csv(path=f"./outputs/{file_name}_{date_time}.csv", mode='overwrite', header=True)

def output_file_df(spark, file_name, df):
    df.coalesce(1).write.csv(path=f"./outputs/{file_name}_{date_time}.csv", mode='overwrite', header=True)


def initialize_database(db_type):
    if db_type == "source":
        try:
            db = DBAccess(os.getenv('DB_HOST'), os.getenv('DB_DATABASE'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
            db.connect()
            logger.info("Successfully connected to the database.")
            return db
        except Exception as e:
            logger.error(f"An unexpected error occurred during database initialization: {e}")
            return None
    if db_type == "target":
        try:
            db = DBAccess(os.getenv('DB_TARGET_HOST'), os.getenv('DB_TARGET_DATABASE'), os.getenv('DB_TARGET_USER'), os.getenv('DB_TARGET_PASSWORD'))
            db.connect()
            logger.info("Successfully connected to the database.")
            return db
        except Exception as e:
            logger.error(f"An unexpected error occurred during database initialization: {e}")
            return None




def clean_data():
    return

def filter_data(spark, df):

    names_df, names_with_titles_df = filter_titles(df)
    output_file_df(spark, "Filtered_FirstNameTitles", names_with_titles_df)

    names_df, multiple_names_df = filter_multiple_collectors(names_df)
    output_file_df(spark, "Filtered_CollectorTeams", multiple_names_df)

    names_df, organizations_df = filter_institutions(names_df)
    output_file_df(spark, "Filtered_Institutions", organizations_df)
    return names_df



def enrich_data(spark, names_df):
    spark.sparkContext.addPyFile('./resource_api.zip')
    import resource_api
    def search_wrapper(orcid_search = OrcidAPI()):
        def partition_search_function(names_df):
            return partition_search(orcid_search, names_df)
        return partition_search_function

    search_function = search_wrapper()
    rdd = names_df.rdd.mapPartitions(search_function)
    extracted_names_rdd = rdd.mapPartitions(process_search_name_iterator)

    return extracted_names_rdd


def fuzzy_match(spark, names_rdd):
    best_matches = names_rdd.mapPartitions(get_best_match_iterator)

    passed_rdd = best_matches.filter(lambda x: x['confidence_score'] is not None and  x['confidence_score'] > 95)
    failed_rdd = best_matches.filter(lambda x: x['confidence_score'] is None or x['confidence_score'] < 95 )

    output_file_rdd(spark, "failed", failed_rdd)
    output_file_rdd(spark, "passed", passed_rdd)

    return passed_rdd



def main():
    #instantiate_error_log('dropped_agents.csv')


    source_db = initialize_database('source')
    names_to_search = source_db.fetch_collectors()

    spark = SparkSession.builder.appName("CollectorNamePreprocessing").getOrCreate()
   # spark.sparkContext.setLogLevel("DEBUG")

    df = spark.createDataFrame(names_to_search, schema=['first_name', 'middle_name', 'last_name', 'title', 'agend_id'])

    processed_df = filter_data(spark, df) #preprocessing based on given headers to handle poorly formatted strings or rows that arent present

    extracted_names_rdd = enrich_data(spark, processed_df)

    fuzzy_matched_collectors = fuzzy_match(spark, extracted_names_rdd)

    if fuzzy_matched_collectors:
        target_db = initialize_database('target')








if __name__ == "__main__":
    main()
