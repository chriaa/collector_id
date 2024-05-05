import logging
import sys

sys.path.append('../')  # Adjust the path as necessary

from api.OrcidAPI import OrcidAPI
from api.OpenAlexAPI import OpenAlexAPI
from database.DBAccess import DBAccess
from utils.CleanData import CleanData, process_search_name_data, clean_name
from utils.FuzzyMatchNames import FuzzyMatchNames, get_best_match_and_confidence
from dotenv import load_dotenv

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
    # Connect to database
    db = initialize_application()
    df = pd.DataFrame()

    # Get the names of collectors from the database
    names_to_search = db.fetch_collectors()

    for name in names_to_search:

        #readapt clean_name function to:
        # extract the number of individual names (for example there is an and)
        # and to search if there is a nickname associate with a name
        cleaned_name = clean_name(name[:3])
        print("original_name: {name}, cleaned_name: {cleaned_name}".format(name=name, cleaned_name=cleaned_name))

       for




        # Authenticate Orcid API using Credentials
        orcid_api = OrcidAPI()
        # alex_api = OpenAlexAPI()

        redirect_uri = 'https://127.0.0.1/'
        # Exchange the authorization code for an access token
        code = 'de615df9-a1ce-4481-8ccd-efdb323d7874'
        # access_token = orcid_api.exchange_code_for_token(code, redirect_uri)
        """

        if access_token:
            logger.info("Successfully obtained access token.")
            # Continue with using the access token for further API calls
        else:
            logger.error("Failed to obtain access token.")



        orcid_matches =[]
        if access_token:
            orcid_matches = orcid_api.search_orcid(cleaned_name)
            print(orcid_matches)
        else:
            print("Failed to obtain access token.")
        """

        orcid_matches = orcid_api.search_orcid(cleaned_name, code)

        # Process the matches if there are any, and proceed only if the processed data is not empty
        if orcid_matches and 'expanded-result' in orcid_matches and orcid_matches['expanded-result']:
            processed_matches = process_search_name_data(orcid_matches, cleaned_name)
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


if __name__ == "__main__":
    main()
