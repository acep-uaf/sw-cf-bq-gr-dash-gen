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

 ### Deployment
 
 Deploy the Cloud Function with the provided shell script:
 
 ```bash
 ./eiedeploy.sh
 ```
 
 This script wraps the following `gcloud` command:

 #!/bin/bash

# Source the .env file
source eiedeploy.env

```
# Deploy the function
gcloud functions deploy sw-cw-bq-gr-dash-gen \
   --$GEN2 \
   --runtime=$RUNTIME \
   --region=$REGION \
   --source=$SOURCE \
   --entry-point=$ENTRY_POINT \
   --memory=$MEMORY \
   --timeout=$TIMEOUT  \
   --trigger-topic=$TRIGGER_TOPIC \
   --set-env-vars TEMPLATE_BUCKET=$TEMPLATE_BUCKET,TEMPLATE_JSON=$TEMPLATE_JSON,ARCHIVE_BUCKET=$ARCHIVE_BUCKET,PUBSUB_TOPIC=$PUBSUB_TOPIC
```

### .env File Configuration
 
Before deploying the Cloud Function, ensure that the `eiedeploy.env` file contains the necessary environment variables, as the deployment script sources this file. This file should define values for:

```
GEN2=<value>
RUNTIME=<value>
REGION=<value>
SOURCE=<value>
ENTRY_POINT=<value>
MEMORY=<value>
TIMEOUT=<value>
TRIGGER_TOPIC=<value>
TEMPLATE_BUCKET=<value>
TEMPLATE_JSON=<value>
ARCHIVE_BUCKET=<value>
PUBSUB_TOPIC=<value>
```

Replace `<value>` with the appropriate values for your deployment.
 
### Environment Variable Descriptions
 
Below are descriptions for each environment variable used in the deployment script:

- **GEN2**=`<value>`:
  - Description: Specifies the generation of the Cloud Function to deploy. For example: `gen2` when you intend to deploy a second generation Google Cloud Function.
  
- **RUNTIME**=`<value>`:
  - Description: Specifies the runtime environment in which the Cloud Function executes. For example: `python311` for Python 3.11.
  
- **REGION**=`<value>`:
  - Description: The Google Cloud region where the Cloud Function will be deployed and run. Example values are `us-west1`, `europe-west1`, etc.
  
- **SOURCE**=`<value>`:
  - Description: Path to the source code of the Cloud Function. Typically, this points to a directory containing all the necessary files for the function.
  
- **ENTRY_POINT**=`<value>`:
  - Description: Specifies the name of the function or method within the source code to be executed when the Cloud Function is triggered.
  
- **MEMORY**=`<value>`:
  - Description: The amount of memory to allocate for the Cloud Function. This is denoted in megabytes, e.g., `16384MB`.
  
- **TIMEOUT**=`<value>`:
  - Description: The maximum duration the Cloud Function is allowed to run before it is terminated. Expressed in seconds, e.g., `540s`.
  
- **TRIGGER_TOPIC**=`<value>`:
  - Description: The Google Cloud topic under which the Cloud Function is subscribed.
  
- **TEMPLATE_BUCKET**=`<value>`:
  - Description: Specifies the Cloud Storage bucket where the JSON template file for the dashboard is stored.
  
- **TEMPLATE_JSON**=`<value>`:
  - Description: The name of the JSON template file in the TEMPLATE_BUCKET to be used for the dashboard.
  
- **ARCHIVE_BUCKET**=`<value>`:
  - Description: Specifies the Cloud Storage bucket where the updated JSON file (serving as an archive) is written after processing.
  
- **PUBSUB_TOPIC**=`<value>`:
  - Description: The name of the Pub/Sub topic to which the Cloud Function publishes messages.

Set each `<value>` in the `eiedeploy.env` file appropriately before deploying the Cloud Function. **Note:** For security reasons, do not cheeck the `eiedeploy.env` with values     set  into a public repository such as github.





## Dependencies
The function relies on the following packages, as outlined in the `requirements.txt` file:
- google-cloud-bigquery
- google-cloud-storage

## Conclusion
sw-cw-bq-gr-dash-gen is a sophisticated Cloud Function tailored to interact with various Google Cloud Services. It successfully extracts, transforms, and loads data into a dashboard template, providing valuable insights through an automated, scalable, and robust solution.

