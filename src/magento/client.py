import logging
import os
import requests
from urllib.parse import urljoin
from kbc.client_base import HttpClientBase


class MagentoClient(HttpClientBase):

    def __init__(self, apiUrl, token):

        self.parApiUrl = urljoin(apiUrl, 'rest')
        self.parToken = token

        _defaultHeader = {
            'Authorization': f'Bearer {self.parToken}'
        }

        super().__init__(self.parApiUrl, default_http_header=_defaultHeader)
        logging.debug(self.base_url)

    def put_raw(self, *args, **kwargs):

        s = requests.Session()
        headers = kwargs.pop('headers', {})
        headers.update(self._auth_header)
        s.headers.update(headers)
        s.auth = self._auth

        r = self.requests_retry_session(session=s).request('PUT', *args, **kwargs)
        return r

    def sendPostRequest(self, endpoint, method, data):

        urlRequest = os.path.join(self.base_url, endpoint)
        logging.debug(urlRequest)
        headersRequest = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        return self.post_raw(url=urlRequest, json=data, headers=headersRequest)

    def sendPutRequest(self, endpoint, method, data):

        urlRequest = os.path.join(self.base_url, endpoint)
        logging.debug(urlRequest)
        headersRequest = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        return self.put_raw(url=urlRequest, json=data, headers=headersRequest)
