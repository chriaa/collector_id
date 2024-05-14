import logging
import os
import sys
from pathlib import Path

from pyspark import rdd

#sys.path.append('../')  # Adjust the path as necessary
sys.path.append(str(Path(__file__).resolve().parent.parent))

from resource_api.OrcidAPI import OrcidAPI, partition_search
from resource_api.OpenAlexAPI import OpenAlexAPI
from database.DBAccess import DBAccess
from utils.CleanData import (CleanData, process_search_name_data, clean_name, test_name_filtering, instantiate_error_log,
                             filter_titles_pyspark, filter_multiple_collectors, filter_institutions, test_process_search_name_iterator)
from utils.FuzzyMatchNames import FuzzyMatchNames, get_best_match_and_confidence
from dotenv import load_dotenv


from pyspark.sql import SparkSession
import pandas as pd

# Load the environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_application():
    try:
        db = DBAccess()
        db.connect()
        logger.info("Successfully connected to the database.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during database initialization: {e}")
        return None
    return db


def main():
    instantiate_error_log('dropped_agents.csv')
    # Connect to database
    db = initialize_application()
    df = pd.DataFrame()

    # Get the names of collectors from the database
    names_to_search = db.fetch_collectors() #returns a list of tuples



    ########
   # test_names_to_search = list(zip(*names_to_search))
    test_names_to_search = names_to_search
    columns = ['first_name', 'middle_name', 'last_name', 'title', 'agend_id']

    spark = SparkSession.builder.appName("CollectorNamePreprocessing").getOrCreate()
   # spark.sparkContext.setLogLevel("DEBUG")
    test_df = spark.createDataFrame(test_names_to_search, schema=columns)
    test_df.show()
    #spark.stop()

    names_df, names_with_titles_df = filter_titles_pyspark(test_df)

    # log

    names_df, multiple_names_df = filter_multiple_collectors(names_df)

    #log

    names_df, organizations_df = filter_institutions(names_df)

    #log



    #end of data processing

    #########

    spark.sparkContext.addPyFile('./resource_api.zip')
    import resource_api

    def search_wrapper(orcid_search = OrcidAPI()):
        def partition_search_function(names_df):
            return partition_search(orcid_search, names_df)
        return partition_search_function



    search_function = search_wrapper()

    #   Output Names

    rdd = names_df.rdd.mapPartitions(search_function)
    #orcid_search_results = rdd.collect()
    #print("these are the results: ", orcid_search_results)
    extracted_names_rdd = rdd.mapPartitions(test_process_search_name_iterator)

    #   output current search results
    print("these are the extracted names")
    print(extracted_names_rdd.collect())

    #   Fuzzy Matching

    '''

    for name in names_to_search:

        first_name, middle_name, last_name = clean_name(name[:3])

        first_name = first_name.replace("&", "and") #need to do this so punctuation doesnt drop it

        cleaned_name = f"{first_name} {middle_name} {last_name}"


        new_first_name, new_middle_name, new_last_name = test_name_filtering(first_name, middle_name, last_name, name)

        if new_first_name and new_middle_name and new_last_name:
            filtered_name = f"{new_first_name} {new_middle_name} {new_last_name}"
            print("original_name: {name}, cleaned_name: {cleaned_name} filtered_name: {filtered_name}".format(name=name,
                                                                                                              cleaned_name=cleaned_name,
                                                                                                              filtered_name=filtered_name))
        else:
            print("original_name: {name}, cleaned_name: {cleaned_name}".format(name=name, cleaned_name=cleaned_name))

    print(len(names_to_search))
    '''

    '''
    for name in names_to_search:

        # Authenticate Orcid API using Credentials
        orcid_api = OrcidAPI()
        # alex_api = OpenAlexAPI()

        redirect_uri = 'https://127.0.0.1/'
        # Exchange the authorization code for an access token
        code = os.getenv('ACCESS_TOKEN')
        # access_token = orcid_api.exchange_code_for_token(code, redirect_uri)

        orcid_matches = orcid_api.search_orcid(cleaned_name, code)

        # Process the matches if there are any, and proceed only if the processed data is not empty
        if orcid_matches and 'expanded-result' in orcid_matches and orcid_matches['expanded-result']:

            processed_matches = process_search_name_data(orcid_matches, cleaned_name)


            print("these were processed normally: ", processed_matches)


            if processed_matches:  # Check if there are any processed matches to avoid creating an empty DataFrame
                df = pd.DataFrame(processed_matches)
                # df['full_name'] = df.apply(lambda x: f"{x['Given Names']} {x['Family Names']}".strip(), axis=1)
                df['full_name'] = df.apply(lambda x: (f"{x['Given Names']} {x['Family Names']}").strip(' "\''), axis=1)

                # Fuzzy match and confidence score calculation
                df[['Best Match Column', 'Best Match Name', 'Confidence Score']] = df.apply(
                    lambda x: get_best_match_and_confidence(x['CAS Botany Name'], x['full_name'], x['Credit Names'],
                                                            x['Other Names']),
                    axis=1, result_type='expand'
                )
                df.dropna(subset=['Best Match Column', 'Best Match Name', 'Confidence Score'], inplace=True)

            else:
                print("No valid processed matches found. Check data quality or parameters.")
        else:
            print("No results found from ORCID search or no valid data to process.")

    # Save rows that will be dropped due to NaN values in specified columns
    rows_to_drop = df[df[['Best Match Column', 'Best Match Name', 'Confidence Score']].isna().any(axis=1)]

    # Drop the NaN values from the original DataFrame
    df.dropna(subset=['Best Match Column', 'Best Match Name', 'Confidence Score'], inplace=True)

    # Output the rows to be dropped
    print("Rows dropped for not having a best match:")
    print(rows_to_drop)

    # Output the cleaned DataFrame
    print("\nCleaned DataFrame:")
    print(df)
    '''

if __name__ == "__main__":
    main()
