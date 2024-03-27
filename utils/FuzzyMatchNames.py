from fuzzywuzzy import process

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



if __name__ == "__main__":
    fuzzy_match = FuzzyMatchNames()



