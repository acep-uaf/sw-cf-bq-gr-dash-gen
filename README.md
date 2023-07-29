# SW-CW-BQ-GR-DASH-GEN Cloud Function

The sw-cw-bq-gr-dash-gen is a Cloud Function meticulously designed to create a dashboard by executing a series of BigQuery queries. This complex operation involves querying time-series data with conditional count logic, manipulating data, and rendering it in a pre-defined template for visualization. It also utilizes BigQuery and Cloud Storage services, providing an end-to-end solution for dashboard generation.

## Cloud Function
### Description
The gen 2 Cloud Function mk_gr_dsh is a Python function implemented with Python 3.11 runtime. It leverages the `google.cloud.bigquery` and `google.cloud.storage` libraries to interact with Google's BigQuery service and Cloud Storage, respectively. The function is triggered by an event from a Pub/Sub topic.

## Event Handling
Upon receiving the event, the function extracts and decodes the base64-encoded data from the event payload, converting it into a JSON string. It then maps this JSON into a dictionary, which allows it to fetch vital information like the `project_id`, `dataset_id`, and `table_id`. If any of these crucial parameters are missing, the function will halt, printing an error message.

## Query Formation
Based on the provided parameters, the function forms a BigQuery URI and constructs a comprehensive SQL query. This query contains complex subqueries and conditional logic to count specific measurements such as voltage, frequency, and current across different components, filtered by constraints.

The primary query consists of three main parts:
- **time_series:** Generating timestamps with an hourly interval between the minimum and maximum datetime from the given dataset.
- **data:** Aggregating count data based on specific measurement names with several constraints.
- **Main query:** Filtering the data by applying specific count thresholds and ordering the results.
The query is limited to retrieving five records, aligned with a defined constraint.

## Query Execution
Once the query is defined, it's executed through BigQuery's client. The result is iterated to find the first timestamp, which is then formatted into a string.

## JSON Template Handling
The function then proceeds to download a JSON template file from the specified Cloud Storage bucket. It manipulates this template by replacing placeholders and setting a specific time range based on the results of the query. The title of the template is also updated.

## Archive Writing
After modifying the JSON template, it's written back to a new Cloud Storage bucket. The updated JSON file serves as an archive and follows a naming convention based on the `dataset_id`.

## Exception Handling
The function includes a robust exception handling mechanism, capturing specific exceptions like `Forbidden`, `BadRequest`, and `GoogleAPICallError`, as well as handling any unexpected errors. Each of these is logged with a descriptive message to facilitate debugging.

## Deployment
Deployment of this function is performed using a shell script and the `gcloud` command-line tool. The function is configured with specific settings, including runtime, region, source, entry-point, memory allocation, timeout, trigger topic, and environment variables. Key environment variables like `TEMPLATE_BUCKET`, `TEMPLATE_JSON`, and `ARCHIVE_BUCKET` are set during the deployment.

## Dependencies
The function relies on the following packages, as outlined in the `requirements.txt` file:
- google-cloud-bigquery
- google-cloud-storage

## Conclusion
The sw-cw-bq-gr-dash-gen is a sophisticated Cloud Function tailored to interact with various Google Cloud Services. It successfully extracts, transforms, and loads data into a dashboard template, providing valuable insights through an automated, scalable, and robust solution.

