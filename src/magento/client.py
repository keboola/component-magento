import logging
import os
# import sys
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

    def sendPostRequest(self, endpoint, method, data):

        urlRequest = os.path.join(self.base_url, endpoint)
        logging.debug(urlRequest)
        headersRequest = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        return self.post_raw(url=urlRequest, json=data, headers=headersRequest)
