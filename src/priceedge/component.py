import csv
import glob
import json
import logging
import os
import sys
from kbc.env_handler import KBCEnvHandler
from priceedge.client import PriceEdgeClient

KEY_APIURL = 'api_url'
KEY_USERNAME = 'username'
KEY_PASSWORD = '#password'
KEY_CLIENTID = 'client_id'

MANDATORY_PARAMETERS = [KEY_APIURL, KEY_USERNAME, KEY_PASSWORD, KEY_CLIENTID]


class PriceEdgeComponent(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMETERS)
        self.validate_config(mandatory_params=MANDATORY_PARAMETERS)
        self.getAndCheckInputTable()

        self.parApiUrl = self.cfg_params[KEY_APIURL]
        self.parUsername = self.cfg_params[KEY_USERNAME]
        self.parPassword = self.cfg_params[KEY_PASSWORD]
        self.parClientId = self.cfg_params[KEY_CLIENTID]

        self.client = PriceEdgeClient(apiUrl=self.parApiUrl,
                                      username=self.parUsername,
                                      password=self.parPassword,
                                      clientId=self.parClientId)

    def getAndCheckInputTable(self):

        globString = os.path.join(self.tables_in_path, '*.csv')
        inputTables = glob.glob(globString)

        if len(inputTables) == 0:
            logging.error("No input tables specified.")
            sys.exit(1)

        elif len(inputTables) != 1:
            logging.error("More than 1 input table was specified. One table import is allowed.")
            sys.exit(1)

        else:
            tablePath = inputTables[0]
            self.reader = csv.DictReader(open(tablePath))

            if 'endpoint' not in self.reader.fieldnames or 'data' not in self.reader.fieldnames:
                logging.error("Required field \"endpoint\" or \"data\" is missing in the input table.")
                sys.exit(1)

            outputFields = self.reader.fieldnames + ['request_status', 'request_message']
            outputPath = os.path.join(self.tables_out_path, 'result.csv')
            self.writer = csv.DictWriter(open(outputPath, 'w'), fieldnames=outputFields,
                                         restval='', extrasaction='ignore',
                                         quotechar='"', quoting=csv.QUOTE_ALL)
            self.writer.writeheader()

    def sendCall(self):

        for row in self.reader:
            logging.debug(row)

            reqEndpoint = row['endpoint']
            if reqEndpoint.startswith('/'):
                reqEndpoint = reqEndpoint[1:]

            try:
                reqData = json.loads(row['data'])

            except ValueError as e:
                self.varErrorCounter += 1
                self.writer.writerow({
                    **row,
                    **{
                        'request_status': "JSON_ERROR",
                        'request_message': f"Invalid JSON detected in data. {e}"
                    }
                })

                continue

            request = self.client.sendRequest(reqEndpoint, reqData)
            logging.debug("request returned")
            logging.debug(request)
            logging.debug(request.status_code)

            scRequest = request.status_code
            jsRequest = request.json()
            if scRequest != 200:
                self.varErrorCounter += 1
                self.writer.writerow({
                    **row,
                    **{
                        'request_status': "REQUEST_ERROR",
                        'request_message': jsRequest
                    }
                })

            else:
                errors = jsRequest.get('Errors', [])

                if len(errors) == 0:
                    self.writer.writerow({
                        **row,
                        **{
                            'request_status': "SUCCESS"
                        }
                    })

                else:
                    self.writer.writerow({
                        **row,
                        **{
                            'request_status': "REQUEST_ERROR",
                            'request_message': errors
                        }
                    })

    def run(self):

        self.varErrorCounter = 0
        self.sendCall()

        if self.varErrorCounter > 0:
            logging.warn(f"There were {self.varErrorCounter} errors during process.")
