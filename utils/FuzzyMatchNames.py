from fuzzywuzzy import process, fuzz
import pandas as pd


class FuzzyMatchNames:
    def __init__(self):
        self.fuzzy_match_names = []

    def get_top_three_matches(query_name, possible_matches):
        """
        Performs fuzzy matching to find the top three names in the list of possible matches
        that are closest to the given query name.

        :param query_name: The name to search for.
        :param possible_matches: A list of names to compare against the query name.
        :return: A list of the top three closest matches with their scores.
        """
        top_three = process.extract(query_name, possible_matches, limit=3)
        return top_three


'''
    PySpark MapPartitions Iterator that takes an individual record as input 

'''


def get_best_match_iterator(iterator):
    results = []

    for record in iterator:

        key = 'processed_potential_matches'

        if isinstance(record, dict) and key in record:
            potential_matches = record[key]
            for e in potential_matches:
                best_match_key, best_match_value, confidence_score = get_best_match_and_confidence(e['Searched Name'],
                                                                                                   f"{e['Given Names']} {e['Family Names']}",
                                                                                                   e['Credit Names'],
                                                                                                   e['Other Names'])
                # agent_id
                cas_name = e['Searched Name']
                # a record should still be generated regardless of whether the name clears the benchmark
                # so that the record can be properly recorded as being searched but no records being found
                result = {
                    'full_name': cas_name,
                    'best_match_source_field': best_match_key,
                    'target_name': best_match_value,
                    'match_confidence': confidence_score,
                    'agent_id': record['agent_id'],
                    'orcid_id': e['ORCID ID']
                }
                # cas_agent_id = e['agent_id']
                results.append(result)  # probably should be a dictionar

    return iter(results)


def get_best_match_and_confidence(cas_name, full_name, credit_names, other_names):
    """
    Compares the CAS Botany Name against full_name, credit names, and other names,
    selects the best match, and calculates the confidence score.
    """

    # Prepare choices and include full_name by default
    choices = {'full_name': full_name}

    # Handle credit_names if provided
    if pd.notna(credit_names):
        choices['credit_names'] = credit_names

    # Handle other_names if provided and assuming it's now received as a list
    if other_names:  # Checks if other_names is a non-empty list directly
        for i, other_name in enumerate(other_names):
            if other_name:  # Ensure it's not an empty string
                choices[f'other_names_{i}'] = other_name

    # Conduct the fuzzy matching against all available names and returns the best one
    scores = {key: fuzz.ratio(cas_name, value) for key, value in choices.items()}
    best_match_key = max(scores, key=scores.get)
    best_match_value = choices[best_match_key]
    confidence_score = scores[best_match_key]

    if confidence_score < 50:
        return None, None, None
        # Return the column of the best match, the best matched name itself, and the confidence score
    return best_match_key, best_match_value, confidence_score


if __name__ == "__main__":
    fuzzy_match = FuzzyMatchNames()
