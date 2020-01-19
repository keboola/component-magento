import logging
import os
import requests
import sys
from kbc.client_base import HttpClientBase


class PriceEdgeClient(HttpClientBase):

    def __init__(self, apiUrl, username, password, clientId):

        self.parApiUrl = apiUrl
        self.parUsername = username
        self.parPassword = password
        self.parClientId = clientId

        super().__init__(self.parApiUrl)
        self.patch_raw = requests.put
        self.getToken()

    def getToken(self):

        urlToken = os.path.join(self.base_url, 'token')
        headerToken = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        bodyToken = {
            'grant_type': 'password',
            'username': self.parUsername,
            'password': self.parPassword,
            'client_id': self.parClientId
        }

        reqToken = self.post_raw(urlToken, headers=headerToken, data=bodyToken)
        scToken, jsToken = reqToken.status_code, reqToken.json()

        if scToken == 200:

            logging.info("Access token acquired.")
            self.varToken = jsToken['access_token']
            self._auth_header = {
                'Authorization': f'Bearer {self.varToken}',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

        else:

            logging.error(f"Could not get access token. Received: {scToken} - {jsToken}.")
            sys.exit(1)

    def sendRequest(self, endpoint, body):

        urlRequest = os.path.join(self.base_url, endpoint)
        bodyRequest = body

        reqRequest = self.patch_raw(url=urlRequest, headers=self._auth_header, json=bodyRequest)
        return reqRequest
