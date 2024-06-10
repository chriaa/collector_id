** Collector Identification Pipeline **



## Overview

* Connects to the collections database.
* Retrieves all collector records.
* Searches each collector against ORCID's API to obtain an ORCID number.
* Performs a fuzzy match between the searched name and the ORCID search result name.
* Uploads all records above a certain threshold into the database.


## Prerequisites

To run this project, you will need:

* Copy of Casbotany database
* ORCID API credentials
* Python 3.12.3
* Docker

## Installation

1. Clone the repository
2. Install the required Python libraries

## Configuration

1. Sign up for an ORCID account and register an application with ORCID's Developer Tools. Provide the necessary tokens/info in the `.env` file.
2. The pipeline has authorization instantiation capabilities but does not yet handle obtaining the access token (this is a to-be-done task). In the meantime, you may use Postman to get the access token.


## Usage

1. Fill out the `.env` file variables.
2. Fill out the `create_db.sh` file variables.
3. Run the `create_db.sh` file to create the target database (a MySQL Docker database)
4. Run the main pipeline script

## Fuzzy Matching Threshold

The default threshold for fuzzy matching is set in the `get_best_match_and_confidence` function in the `fuzzymatch` class (currently set at 50). You can adjust this threshold as needed. You can also define the pass and fail standards for what gets uploaded to the target database by editing the fuzzy match section in `main.py`.

(Note: I know this can be consolidated, but now is not the time.)

## Error Handling

The pipeline handles errors or failed API calls by generating a CSV of names that didn't pass each filter step. Otherwise, lol good luck.

## Future Improvements

It would be nice to introduce this pipeline to the Collection Explorer ecosystem or improve it in such a way that we can create additional ETL pipelines that might help us create other data sources.
