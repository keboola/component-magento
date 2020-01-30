import csv
import glob
import json
import logging
import os
import sys
from kbc.env_handler import KBCEnvHandler
from magento.client import MagentoClient

KEY_APIURL = 'api_url'
KEY_TOKEN = '#token'

MANDATORY_PARAMETERS = [KEY_APIURL, KEY_TOKEN]
MANDATORY_INPUTFIELDS = set(['endpoint', 'method', 'data'])
SUPPORTED_METHODS = ['POST', 'PUT']


class MagentoComponent(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMETERS)
        self.validate_config(mandatory_params=MANDATORY_PARAMETERS)

        self.parApiUrl = self.cfg_params[KEY_APIURL]
        self.parToken = self.cfg_params[KEY_TOKEN]

        self.getAndCheckInputTable()
        self.client = MagentoClient(apiUrl=self.parApiUrl, token=self.parToken)

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

            missFields = MANDATORY_INPUTFIELDS - set(self.reader.fieldnames)
            if len(missFields) != 0:
                logging.error(f"Missing required fields {list(missFields)} in input table.")
                sys.exit(1)

            outputFields = self.reader.fieldnames + ['request_status', 'request_message', 'request_code']
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

            reqMethod = row['method']

            if reqMethod not in SUPPORTED_METHODS:
                self.writer.writerow({
                    **row,
                    **{
                        'request_status': "METHOD_ERROR",
                        'request_message': f"Unsupported method {reqMethod} detected. Supported: {SUPPORTED_METHODS}.",
                        'request_code': ''
                    }
                })

                continue

            try:
                reqData = json.loads(row['data'])

            except ValueError as e:
                self.writer.writerow({
                    **row,
                    **{
                        'request_status': "JSON_ERROR",
                        'request_message': f"Invalid JSON detected in data. {e}",
                        'request_code': ''
                    }
                })

                continue

            if reqMethod == 'POST':
                request = self.client.sendPostRequest(reqEndpoint, reqMethod, reqData)
            elif reqMethod == 'PUT':
                request = self.client.sendPutRequest(reqEndpoint, reqMethod, reqData)
            else:
                pass

            scRequest = request.status_code
            jsRequest = request.json()

            if request.ok is not True:
                self.writer.writerow({
                    **row,
                    **{
                        'request_status': "REQUEST_ERROR",
                        'request_message': json.dumps(jsRequest),
                        'request_code': scRequest
                    }
                })

            else:
                self.writer.writerow({
                    **row,
                    **{
                        'request_status': "REQUEST_OK",
                        'request_message': json.dumps(jsRequest),
                        'request_code': scRequest
                    }
                })

    def run(self):
        self.sendCall()
