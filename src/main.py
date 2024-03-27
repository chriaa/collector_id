import logging
import sys
from api import OrcidAPI
from api import OpenAlexAPI
from database import DBAccess
from utils import FuzzyMatchNames
from utils import CleanData
from dotenv import load_dotenv

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

    # Get the names of collectors from the database
    names_to_search = db.fetch_collectors()

    for name in names_to_search:

        cleaned_name = CleanData.clean_name(name)

        orcid_api = OrcidAPI()
        alex_api = OpenAlexAPI()

        orcid_matches = orcid_api.search(cleaned_name)
        alex_matches = alex_api.search(cleaned_name)

        best_match_x = perform_fuzzy_matching(cleaned_name, orcid_matches)
        best_match_y = perform_fuzzy_matching(cleaned_name, alex_matches)

        logger.info(f"Best match for '{name}' from XApi: {best_match_x}")
        logger.info(f"Best match for '{name}' from YApi: {best_match_y}")


if __name__ == "__main__":
    main()
