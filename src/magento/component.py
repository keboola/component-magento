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
REQUEST_STATUS_COLUMNS = set(['request_status', 'request_message', 'request_code'])
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

        else:

            _allColumns = set()
            for tablePath in inputTables:

                _tn = os.path.basename(tablePath)
                _rdr = csv.DictReader(open(tablePath))

                missFields = MANDATORY_INPUTFIELDS - set(_rdr.fieldnames)
                if len(missFields) != 0:
                    logging.error(f"Missing required fields {list(missFields)} in input table {_tn}.")
                    sys.exit(1)

                rsrvFields = REQUEST_STATUS_COLUMNS.intersection(set(_rdr.fieldnames))
                if len(rsrvFields) != 0:
                    logging.error(f"Reserved fields {rsrvFields} present in table {_tn}.")
                    sys.exit(1)

                _allColumns = _allColumns.union(_rdr.fieldnames)

            _allColumns = list(_allColumns) + list(REQUEST_STATUS_COLUMNS)
            outputPath = os.path.join(self.tables_out_path, 'result.csv')
            self.writer = csv.DictWriter(open(outputPath, 'w'), fieldnames=_allColumns,
                                         restval='', extrasaction='ignore',
                                         quotechar='"', quoting=csv.QUOTE_ALL)
            self.writer.writeheader()

        self.varInputTablePaths = inputTables

    def sendCall(self):

        for path in self.varInputTablePaths:

            logging.info(f"Writing data from table {os.path.basename(path)}.")
            _rdr = csv.DictReader(open(path))

            for row in _rdr:
                # logging.debug(row)

                reqEndpoint = row['endpoint']
                if reqEndpoint.startswith('/'):
                    reqEndpoint = reqEndpoint[1:]

                reqMethod = row['method']

                if reqMethod not in SUPPORTED_METHODS:
                    self.writer.writerow({
                        **row,
                        **{
                            'request_status': "METHOD_ERROR",
                            'request_message': f"Unsupported method {reqMethod} detected. " +
                            f"Supported methds: {SUPPORTED_METHODS}.",
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

                try:
                    jsRequest = request.json()

                except ValueError:
                    jsRequest = {'response': request.text}

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
