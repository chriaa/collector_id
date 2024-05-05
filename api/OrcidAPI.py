import csv
import requests
from dotenv import load_dotenv
import os


# Accessing variables
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')


class OrcidAPI:

    def __init__(self):
        self.base_url = "https://api.orcid.org"
        self.client_id = client_id
        self.client_secret = client_secret
    # Function to get an access token using client credentials
    def get_access_token(self):
        token_url = "https://orcid.org/oauth/token"
        headers = {'Accept': 'application/json'}
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials',
            'scope': '/read-public'
        }
        response = requests.post(token_url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()['access_token']
        else:
            print(f"Error obtaining access token: {response.text}")
            return None
    # Function to build authorization URL (Manual step for user)
    def get_authorization_url(self, redirect_uri):
        base_url = "https://orcid.org/oauth/authorize"
        scope = "/read-public"
        response_type = "code"
        authorization_url = f"{base_url}?client_id={client_id}&response_type={response_type}&scope={scope}&redirect_uri={redirect_uri}"
        return authorization_url

    # Function to exchange code for token
    def exchange_code_for_token(self, code, redirect_uri):
        token_url = "https://orcid.org/oauth/token"
        headers = {'Accept': 'application/json'}
        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': redirect_uri,
            'scope': "/read-public"
        }
        response = requests.post(token_url, headers=headers, data=data)
        if response.status_code == 200:
            print(response.json()['access_token'])
            return response.json()['access_token']
        else:
            print(f"Error obtaining access token: {response.text}")
            return None



    # Modified search function to use the access token
    def search_orcid(self, name, access_token):
        url = f"https://pub.orcid.org/v3.0/expanded-search?q={name}&start=4&rows=6"
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',

        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error during search: {response.text}")
            return None
