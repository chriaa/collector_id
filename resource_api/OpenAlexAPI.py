import requests


class OpenAlexAPI:
    def __init__(self):
        self.base_url = 'https://api.openalex.org/authors'

    def search_authors_by_name(self, author_name):
        """
        Searches for authors by their display name.

        :param author_name: The display name of the author to search for.
        :return: A list of authors that match the search query.
        """
        # Construct the search URL
        search_url = f'{self.base_url}?filter=display_name.search:{author_name}'

        # Make the GET request to the OpenAlex API
        response = requests.get(search_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            return data['results']
        else:
            print(f"Failed to retrieve data from OpenAlex API, Status Code: {response.status_code}")
            return []


# Example usage
if __name__ == "__main__":
    # Create an instance of the OpenAlexAPI class
    openalex_api = OpenAlexAPI()

    # Search for authors with a specific name
    author_name = "Einstein"  # Replace with the name you're searching for
    authors = openalex_api.search_authors_by_name(author_name)

    # Print the authors' details
    for author in authors:
        print(f"ID: {author['id']}")
        print(f"Name: {author['display_name']}")
        print(f"Works Count: {author['works_count']}")
        print("-" * 20)
