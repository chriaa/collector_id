import re
class CleanData:
    def __init__(self):
        return
def clean_name( name_tuple):
    """
    Cleans name data by trimming whitespace, removing unwanted quotation marks,
    capitalizing first letters of each name part, and handling missing middle initials.

    :param name_tuple: A tuple containing the first name, middle initial, and last name.
    :return: A cleaned, formatted string of the full name.
    """
    # Unpack the tuple into variables, handling potential None values gracefully
    first_name, middle_initial, last_name = (part.strip() if part else "" for part in name_tuple)



    # Remove leading and trailing quotation marks and extra spaces from the first and last names
    first_name = first_name.strip(' "\'').capitalize()
    last_name = last_name.strip(' "\'').capitalize()

    # Check if middle initial is present and handle accordingly
    if middle_initial:
        middle_initial = middle_initial.strip().upper() + "."
        full_name = f"{first_name} {middle_initial} {last_name}"
    else:
        full_name = f"{first_name} {last_name}"

    return full_name

def filter_name_data(name):
    return


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