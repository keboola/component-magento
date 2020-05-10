# Magento writer for Keboola

Magento writer allows to write data from Keboola to a Magento instance. The configuration of the writer is done using input parameters and at least 1 input table. In case a 4xx HTTP error is encountered, **the writer does not fail but instead records this encounter in a status table** and in case of 5xx HTTP error, the request is retried.

## Configuration

The sample configuration of the extractor, including inputs and outputs, can be found in the [component's repository](https://bitbucket.org/kds_consulting_team/kds-team.wr-magento/src/master/component_config/sample-config/).

### Parameters

The configuration of Magento writer requires 2 parameters.

**Parameters**:

- `api_url` - An URL to the Magento instance, with which the component will communicate. The URL **should not** include `rest` part of the URL, since that is added automatically.
- `#token` - An API token, which will be used to authenticate against the API.

Both of the parameters are required.

### Input Table

Input table specifies to which endpoint the data should be sent. The writer iterates over the table and for each row creates a new request to the API. The writer accepts 1 or more input tables which must contain the following columns:

- `endpoint` - An endpoint, where the data will be written, exactly as shown in API's documentation. An example would be: `all/V1/company/product/update`.
- `method` - One of `PUT` or `POST` based on what is required from the endpoint. If method is different than `PUT` or `POST`, the request will not be sent.
- `data` - A valid JSON object, which should be sent along with the request.

In addition, other columns can be provided in the input table as well and will be copied over to the status table. None of the input tables can contain reserved columns `request_status`, `request_message` and `request_code`.

## Status table

As mentioned in the first part of the documentation, for each request writer records its status and saves it in the status table `result`. The status table contains **exactly the same columns as input table(s)** with addition of 3 columns:

- `request_code` - A HTTP response code for the request.
- `request_status` - A short description of the status of the request. One of `REQUEST_OK` (successful request), `REQUEST_ERROR` (unsuccessful request), `METHOD_ERROR` (unsupported method) or `JSON_ERROR` (body of the request is not a valid JSON).
- `request_message` - A long message, either from the API or the code itself, where the error was detected.

The table is **not** loaded incrementally.