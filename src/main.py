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
from database.DBAccess import DBAccess, insert_collector_record
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

#fix these two

'''
    Output csv files of current df/rdd
    Fix this, its ugly
'''
def output_file_rdd(spark, file_name, rdd):

    if not rdd.isEmpty():
        df = spark.createDataFrame(rdd)
        #df.write.csv(path = f"./{file_name}.csv", mode = 'overwrite', header= True)
        formatted_date_time = date_time.strftime("%m-%d_%H:%M")
        output_dir = f"./outputs/{formatted_date_time}"
        os.makedirs(output_dir, exist_ok=True)
        pandas_df = df.toPandas()
        pandas_df.to_csv(f"{output_dir}/{file_name}.csv", index=False)
        #df.coalesce(1).write.csv(path=f"./tests/data/test_search_data.csv", mode='overwrite', header=True)

def output_file_df(spark, file_name, df):

    formatted_date_time = date_time.strftime("%m-%d_%H:%M")
    output_dir = f"./outputs/{formatted_date_time}"
    os.makedirs(output_dir, exist_ok=True)
    pandas_df = df.toPandas()
    pandas_df.to_csv(f"{output_dir}/{file_name}.csv", index=False)




'''
    Initialize data sources
    
'''
def initialize_database(db_type):
    if db_type == "source":
        try:
            db = DBAccess(os.getenv('DB_HOST'), os.getenv('DB_DATABASE'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_HOST_PORT'))
            db.connect()
            logger.info("Successfully connected to the database.")
            return db
        except Exception as e:
            logger.error(f"An unexpected error occurred during database initialization: {e}")
            return None
    if db_type == "target":
        try:
            db = DBAccess(os.getenv('DB_TARGET_HOST'), os.getenv('DB_TARGET_DATABASE'), os.getenv('DB_TARGET_USER'), os.getenv('DB_TARGET_PASSWORD'), os.getenv('DB_TARGET_PORT'))
            db.connect()
            logger.info("Successfully connected to the database.")
            return db
        except Exception as e:
            logger.error(f"An unexpected error occurred during database initialization: {e}")
            return None


'''
    Preprocess and cleans collector names
    Filter names based on titles, teams, and institutions
'''
def clean_and_filter_data(spark, df):

    names_df, names_with_titles_df = filter_titles(df)
    output_file_df(spark, "Filtered_FirstNameTitles", names_with_titles_df)
    #names_with_titles_df.show()

    names_df, multiple_names_df = filter_multiple_collectors(names_df)
    output_file_df(spark, "Filtered_CollectorTeams", multiple_names_df)
    # multiple_names_df.show()

    names_df, organizations_df = filter_institutions(names_df)
    output_file_df(spark, "Filtered_Institutions", organizations_df)
    #organizations_df.show()

    return names_df


'''
    Enrich data with search results from Orcid and extract relevant information
'''
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


    passed_rdd = best_matches.filter(lambda x: x['match_confidence'] is not None and x['match_confidence'] >= 70)
    failed_rdd = best_matches.filter(lambda x: x['match_confidence'] is None or x['match_confidence'] < 70 )

    failed_df = spark.createDataFrame(failed_rdd)
    print("Here are the failures")
    failed_df.show()

    passed_df = spark.createDataFrame(passed_rdd)
    print("Here are the searched names that passed")
    passed_df.show()


    output_file_rdd(spark, "failed_fuzzymatch", failed_rdd)
    output_file_rdd(spark, "passed_fuzzymatch", passed_rdd)

    return passed_rdd



def main():
    #instantiate_error_log('dropped_agents.csv')

    #   Initialize connection to source database (uncomment when not testing)
    source_db = initialize_database('source')

    #   Fetch collectors (uncomment when not testing)
    names_to_search = source_db.fetch_collectors()

    # Instantiate spark application
    spark = SparkSession.builder.appName("CollectorNamePreprocessing").config("spark.sql.debug.maxToStringFields", 100) \
    .getOrCreate()
   #spark.sparkContext.setLogLevel("DEBUG")
    df = spark.createDataFrame(names_to_search, schema=['first_name', 'middle_name', 'last_name', 'title', 'agent_id'])


    #   Cleaning name data and preprocessing to filter organizations, multiple names, etc, (uncomment when not testing)
    processed_df = clean_and_filter_data(spark, df)

    '''
        For testing purposes (processed_data):
    '''
    '''
    processed_df.write.mode("overwrite").parquet("./tests/data/processed_name_dataframe_45")
    read_processed_df = spark.read.parquet("./tests/data/processed_name_dataframe_45")
    print("Here are the processed results")
    read_processed_df.show()
    processed_df = read_processed_df
    '''




    #   Enrich data with search results from ORCID
    names_rdd = enrich_data(spark, processed_df)

    '''
        For testing purposes: (enriched_data)
        An rdd must be converted into a df into order to be written into a parquet file
    '''
    '''
    names_rdd = names_rdd.collect()
    names_df = spark.createDataFrame(names_rdd)
    names_df.write.mode("overwrite").parquet("./tests/data/enriched_name_dataframe_45")
    names_df = spark.read.parquet("./tests/data/enriched_name_dataframe_45")
    print("Here are the search results")
    names_df.show(3)
    extracted_names_rdd = names_df.rdd
    '''




    #   Fuzzy matching ORCID searched names and comparing
    fuzzy_matched_collectors_rdd = fuzzy_match(spark, extracted_names_rdd)

    '''
        For testing purposes: (fuzzy_match_collectors)
    '''
    '''
    try:
        #fuzzy_matched_collectors_rdd = fuzzy_matched_collectors_rdd.collect()
        fuzzy_match_df = spark.createDataFrame(fuzzy_matched_collectors_rdd)
        #fuzzy_match_df.write.mode("overwrite").parquet("./tests/data/matched_name_dataframe_45")
        fuzzy_match_df = spark.read.parquet("./tests/data/matched_name_dataframe_45")
        print("Here are the fuzzy matched results")
        fuzzy_match_df.show()
        fuzzy_matched_collectors_rdd = fuzzy_match_df.rdd
    except Exception as e:
        print("An error occurred")
    '''




    #   Connect to existing database
    target_db = initialize_database('target')
    target_db.initialize_target_database() #    Name this better

    #fuzzy_matched_collectors_rdd.mapPartitions(target_db.insert_collector_records).count()

    #   Insert match results for downstream analysis
    fuzzy_matched_collectors_rdd.foreach(lambda x: insert_collector_record(x))




if __name__ == "__main__":
    main()
