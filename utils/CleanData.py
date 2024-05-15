import csv
import json
import os
import re
import string

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, lower, get_json_object, lit


class CleanData:
    def __init__(self):
        return


def format_name(name):
    """ Capitalize and clean individual name components. """
    return ' '.join(part.capitalize() for part in name.strip(' "\'').split())


''' Note:

    More information needed on any 'codes' that manual transcribers might be using for dealing with names
    '''


def process_search_name_iterator(iterator):
    results = {}

    for name in iterator:
        extracted_data = process_search_name_data(name)
        if extracted_data:
            results.update(extracted_data)

    return iter([results])


def process_search_name_data(data):
    results = {}

    if 'expanded-result' in data:
        potential_match = data['expanded-result']
        searched_name = data['searched_name'].strip()
        # agentId should be added here along with the full name

        matches = []
        for item in potential_match:
            extracted_data = {
                "Searched Name": searched_name,
                "ORCID ID": item.get('orcid-id'),
                "Given Names": item.get('given-names'),
                "Family Names": item.get('family-names'),
                "Email": item.get('email'),
                "Other Names": item.get('other-name'),
                "Credit Names": item.get('credit-name')
            }
            matches.append(extracted_data)

        results[searched_name] = matches
    # else:
    #    results.append(None)

    return results


def filter_titles(df):
    keywords = [
        'Dr', 'Father', "Reverend", "Capt", "Captain", "Prof", "Sir",
        "Mrs", "Lord", "General", "Consul", "Professor", "Sister",
        "Lt", "Lieutenant", "Lady", "Mme", "Mlle", "Miss", "Mrs", "Ms", "Colonel", "Col"
    ]
    regex_pattern = "^(" + "|".join([f"{keyword.lower()}" for keyword in keywords]) + ")$"
    df = remove_punctuation(
        df)  # move outside to main, this could be a preprocessing step if it were made into a function(df, column(s) to clean)
    filtered_titles = df.filter(~lower(col("first_name")).rlike(regex_pattern))
    dropped_titles = df.filter(lower(col("first_name")).rlike(regex_pattern))

    # filtered_titles.show()
    # dropped_titles.show()
    return filtered_titles, dropped_titles


def filter_multiple_collectors(df):
    separators = ['&', ' and ']
    regex_pattern = "^(" + "|".join([f"{sep}" for sep in separators]) + ")$"
    single_collectors = df.filter(~lower(col("first_name")).rlike(regex_pattern))
    dropped_collectors = df.filter(lower(col("first_name")).rlike(regex_pattern))

    return single_collectors, dropped_collectors


def filter_institutions(df):
    keywords = [
        'university',
        'institute',
        'college',
        'research'
    ]
    regex_pattern = "^(" + "|".join([f"{keyword.lower()}" for keyword in keywords]) + ")$"

    filtered_institutions = df.filter(~lower(col("first_name")).rlike(regex_pattern))
    dropped_institutions = df.filter(lower(col("first_name")).rlike(regex_pattern))

    # filtered_institutions.show()
    # dropped_institutions.show()
    return filtered_institutions, dropped_institutions


'''
    Error Logging Functions
'''


def remove_punctuation(df):
    pattern = r"[^\w\s]"
    new_df = df.withColumn("first_name", regexp_replace("first_name", pattern, ""))
    return new_df


def instantiate_error_log(file_path):
    if os.path.isfile(file_path):
        os.remove(file_path)

    with open(file_path, mode='a', newline='') as file:
        fieldnames = ['name', 'record', 'failure_point']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()


def log_error(agent_id, name, failure_point, file_path='dropped_agents.csv'):
    # Open the file in append mode
    with open(file_path, mode='a', newline='') as file:
        fieldnames = ['agent_id', 'name', 'failure_point']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writerow({'agent_id': agent_id, 'name': name, 'failure_point': failure_point})
