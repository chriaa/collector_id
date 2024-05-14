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


def test_handle_first_name(first_name):
    """ Handle extraction and formatting of names inside and outside of parentheses from the first name. """
    inside = re.findall(r'\(([^)]+)\)', first_name)
    outside = re.sub(r'\([^)]*\)', '', first_name).strip()
    formatted_inside = ' '.join(format_name(name) for name in inside)
    formatted_outside = format_name(outside)
    return formatted_inside, formatted_outside


def handle_middle_name(middle_name):
    """ Format the middle name if present. """
    return " " + format_name(middle_name) if middle_name else ''


def handle_last_name(last_name):
    """ Format the last name. """
    # return last_name.strip(' "\'').capitalize()
    return " " + format_name(last_name).capitalize()


def clean_name(name_tuple: tuple[str]):
    first_name, middle_name, last_name = name_tuple
    # formatted_inside, formatted_outside = handle_first_name(first_name)
    first_name = remove_punctuation(first_name)
    middle_name = remove_punctuation(handle_middle_name(middle_name))
    last_name = remove_punctuation(handle_last_name(last_name))

    # return f"{first_name}{middle_name}{last_name}"
    return first_name, middle_name, last_name


def test_process_search_name_iterator(iterator):
    results = []

    for name in iterator:
        extracted_data = test_process_search_name_data(name)
        results.append(extracted_data)

    return iter(results)


def test_process_search_name_data(data):
    results = []

    if 'expanded-result' in data:
        potential_match = data['expanded-result']
        searched_name = data['searched_name']
        #agentId should be added here along with the full name
        print(potential_match)
        matches = []
        for item in potential_match:
            extracted_data = {
                "Searched Name" : searched_name,
                "ORCID ID": item.get('orcid-id'),
                "Given Names": item.get('given-names'),
                "Family Names": item.get('family-names'),
                "Email": item.get('email'),
                "Other Names": item.get('other-name'),
                "Credit Names": item.get('credit-name')
            }
            matches.append(extracted_data)

        results.append(matches)
    else:
        results.append(None)

    return results


def test_test_process_search_name_data(spark_instance, search_result):
    """Extract relevant information for fuzzy matching names from the returned orcid search"""
    # spark = SparkSession.builder.appName("ExtractSearchResults").getOrCreate()
    df = spark_instance.read.json(spark_instance.sparkContext.parallelize([search_result]))

    if 'expanded-result' in df.columns:
        df = df.withColumn("record", get_json_object(lit(search_result), '$.expanded-result'))

    df = df.limit(5)

    results_df = df.select(
        # lit(cleaned_name).alias("CAS Botany Name"),
        get_json_object("record", '$.searched_name').alias("Searched Name"),
        get_json_object("record", '$.orcid-id').alias("ORCID ID"),
        get_json_object("record", '$.given-names').alias("Given Names"),
        get_json_object("record", '$.family-names').alias("Family Names"),
        get_json_object("record", '$.email').alias("Email"),
        get_json_object("record", '$.other-name').alias("Other Names"),
        get_json_object("record", '$.credit-name').alias("Credit Names")
    )

    results = [row.asDict() for row in results_df.collect()]
    return results


def process_search_name_data(search_result, cleaned_name):
    results = []
    # Iterating through at most the first 5 records or fewer if less available
    for record in search_result['expanded-result'][:5]:
        # Initialize default values in case of missing fields
        email = ''
        orcid_id = ''
        given_names = ''
        family_names = ''
        other_names = []
        credit_names = None

        # Safe extraction of fields with checks
        if 'email' in record and record['email']:
            email = record['email'][0]  # Assuming there is at least one email and it's a list

        if 'orcid-id' in record:
            orcid_id = record['orcid-id']

        if 'given-names' in record:
            given_names = record['given-names']

        if 'family-names' in record:
            family_names = record['family-names']

        if 'other-name' in record:
            other_names = record['other-name']

        if 'credit-name' in record:
            credit_names = record['credit-name']

        # Creating the record dictionary
        record_data = {
            'CAS Botany Name': cleaned_name,
            'ORCID ID': orcid_id,
            'Given Names': given_names,
            'Family Names': family_names,
            'Email': email,
            'Other Names': other_names,
            'Credit Names': credit_names
        }
        results.append(record_data)

    return results


def clean_names_test(query):
    query = query.strip(' "\'')

    # Replace backslashes first to avoid escape conflicts
    query = re.sub(r'\\', r'\\\\', query)

    # Escape other special Solr characters, carefully ordering to avoid re-escaping
    special_chars = ['+', '-', '&&', '||', '!', '(', ')', '{', '}', '[', ']', '^', '~', '*', '?', ':', '/']
    escaped_query = re.sub(r'([{}])'.format(''.join(re.escape(x) for x in special_chars)), r'\\\1', query)

    # Additional handling for specific scenarios like double quotes within the text
    # This replaces any remaining double quotes with escaped quotes
    escaped_query = re.sub(r'"', r'\\"', escaped_query)

    # Handle ampersands separately if they're not meant to be Solr operators
    escaped_query = re.sub(r'&', r'\&', escaped_query)

    return escaped_query


# These are functions for filtering certain names
"""
🌟 filter_*: Filters keywords and returns name
📥 name (str): Name string of the .
📤 Returns: filtered name
🚀 Example: >>> filter(names) -> filtered_name
"""


def filter_titles_pyspark(df):
    keywords = [
        'Dr', 'Father', "Reverend", "Capt", "Captain", "Prof", "Sir",
        "Mrs", "Lord", "General", "Consul", "Professor", "Sister",
        "Lt", "Lieutenant", "Lady", "Mme", "Mlle", "Miss", "Mrs", "Ms", "Colonel", "Col"
    ]
    regex_pattern = "^(" + "|".join([f"{keyword.lower()}" for keyword in keywords]) + ")$"
    df = test_remove_punctuation(df)  # move outside to main
    filtered_titles = df.filter(~lower(col("first_name")).rlike(regex_pattern))
    dropped_titles = df.filter(lower(col("first_name")).rlike(regex_pattern))

    filtered_titles.show()
    dropped_titles.show()
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

    filtered_institutions.show()
    dropped_institutions.show()
    return filtered_institutions, dropped_institutions


def filter_titles(name):
    flagged_keywords = [
        'Dr', 'Father', "Reverend", "Capt", "Captain", "Prof", "Sir",
        "Mrs", "Lord", "General", "Consul", "Professor", "Sister",
        "Lt", "Lieutenant", "Lady", "Mme", "Mlle", "Miss", "Mrs", "Ms"
    ]
    flagged_keywords.sort(key=len, reverse=True)
    pattern = r'\b(' + '|'.join(map(re.escape, flagged_keywords)) + r')\b'
    new_name = re.sub(pattern, '', name, flags=re.IGNORECASE).strip()
    new_name = re.sub(r'\s+', ' ', new_name)
    return new_name


def filter_multiple_names(name):
    separators = ['&', ' and ']
    for sep in separators:
        if sep.lower() in name.lower():
            return name.split(sep)
    return name


def filter_groups(name):
    keywords = [
        'university',
        'institute',
        'college',
        'research'
    ]
    for keyword in keywords:
        if keyword.lower() in name.lower():
            return True
    return name


'''
    Error Logging Functions
'''


def test_remove_punctuation(df):
    pattern = r"[^\w\s]"
    new_df = df.withColumn("first_name", regexp_replace("first_name", pattern, ""))
    return new_df


def remove_punctuation(text):
    translator = str.maketrans('', '', string.punctuation)
    return text.translate(translator)


def test_name_filtering(first_name, middle_name, last_name, original_name_tuple):
    name = f"{first_name}{middle_name}{last_name}"
    # Filtering by titles
    filtered_first_name = filter_titles(first_name)
    if filtered_first_name != name and len(filtered_first_name) == 0:
        log_error(name, original_name_tuple, "First Name is a title")
        return False, False, False

    '''

    # Filtering multiple collectors
    filtered_name = filter_multiple_names(f"{filtered_first_name}{middle_name}{last_name}")
    if isinstance(filtered_name, list):
        log_error(name, original_name_tuple, "There are multiple collectors present")
        print("There are multiple collectors present")
        return False, False, False

    # Filtering groups
    filtered_name = filter_groups(filtered_name)
    if filtered_name != filtered_name:
        log_error(name, original_name_tuple, "This is an organization")
        print("This is an organization")
        return False, False, False
    '''
    return first_name, middle_name, last_name


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
