from google.api_core.exceptions import Forbidden, BadRequest, GoogleAPICallError
from google.cloud import bigquery, storage
import base64
import json
import os

def mk_gr_dsh(event, context):
    print(f'Received event: {event}')  

    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        message_dict = json.loads(pubsub_message)

        project_id = message_dict.get('project_id')
        dataset_id = message_dict.get('dataset_id')
        table_id = message_dict.get('table_id')

        print(f'project id: {project_id}')
        print(f'dataset id: {dataset_id}')
        print(f'table id: {table_id}')

        if not all([project_id, dataset_id, table_id]):
            print('Error: Missing necessary parameters in the message.')
            return
        
        bigquery_uri = f'{project_id}.{dataset_id}.{table_id}'

        print(f'BigQuery URI: {bigquery_uri}')

        client = bigquery.Client(project=project_id)

        # Define the query
        query = f"""
        WITH 
        measurement_names AS (
        SELECT column_name
        FROM UNNEST([
            'DT1-B_VAB', 'DT1-B_VBC', 'DT1-B_VCA',
            'DT2-B_VAB', 'DT2-B_VBC', 'DT2-B_VCA',
            'DT3-B_VAB', 'DT3-B_VBC', 'DT3-B_VCA',
            'GVEA-B_VAB', 'GVEA-B_VBC', 'GVEA-B_VCA',
            'G3-B_VAB', 'G3-B_VBC', 'G3-B_VCA',
            'G5-B_VAB', 'G5-B_VBC', 'G5-B_VCA',
            'GVEA-B_Frequency',
            'DT1-B_IA', 'DT1-B_IB', 'DT1-B_IC'
        ]) AS column_name
        ),
        time_series AS (
        SELECT TIMESTAMP_TRUNC(t, HOUR) as ts_datetime
        FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
            (SELECT MIN(TIMESTAMP(datetime)) FROM `{bigquery_uri}`), 
            (SELECT MAX(TIMESTAMP(datetime)) FROM `{bigquery_uri}`), 
            INTERVAL 1 HOUR)) t
        ),
        data AS (
        SELECT
            TIMESTAMP_TRUNC(TIMESTAMP(datetime), HOUR) as data_datetime,
            measurement_name,
            COUNT(1) as count
        FROM `acep-ext-eielson-2021.2022_11_11.vtndpp`
        WHERE measurement_name IN (SELECT column_name FROM measurement_names)
        GROUP BY 1, 2
        ),
        min_counts AS (
        SELECT data_datetime, MIN(count) as min_count
        FROM data
        GROUP BY data_datetime
        )
        SELECT ts.ts_datetime, mc.min_count
        FROM time_series ts
        JOIN min_counts mc
        ON ts.ts_datetime = mc.data_datetime
        WHERE mc.min_count >= 3000
        ORDER BY ts.ts_datetime ASC
        LIMIT 5
        """
        # Run the query
        query_job = client.query(query)
        #query_job.result()  # Wait for the job to finish
        result = query_job.result()  # Wait for the job to finish


        # Define a variable to keep the first timestamp
        first_ts = None

        # Iterate over the rows in the query result
        for row in result:
            print(f'{row.ts_datetime}, {row.min_count}')
            
            # If first_ts is None, set it to the current timestamp
            if first_ts is None:
                first_ts = row.ts_datetime

        # Convert first_ts to a string in 'yyyy-mm-ddTHH:MM:SS.000Z' format
        first_ts_str = first_ts.strftime('%Y-%m-%dT%H:%M:%S.000Z')

        # Replace "-" with "_" in the date part
        first_ts_str = first_ts_str.replace("-", "_", 2)

        print(f'first TS sting formatted: {first_ts_str}')


        # get the JSON template file
        storage_client = storage.Client()
        template_bucket = storage_client.get_bucket(os.getenv('TEMPLATE_BUCKET'))
        template_blob = template_bucket.blob(os.getenv('TEMPLATE_JSON'))  # renamed variable
        json_template = json.loads(template_blob.download_as_text())  # used renamed variable

        print(f'template bucket: {template_bucket}')
        print(f'template file: {template_blob}')

        for panel in json_template['panels']:
            for target in panel['targets']:
                if 'rawSql' in target:
                    target['rawSql'] = target['rawSql'].replace('dataset_id_place_holder', dataset_id)

        json_date = dataset_id.replace("_", "-") # 'yyyy-mm-dd' format
        
        # Set time range and title in the template
        json_template['time']['from'] = json_date + 'T09:00:00.000Z'
        json_template['time']['to'] = json_date + 'T09:59:59.000Z'
        json_template['title'] = 'SW Grid Base Line - VT&D - Bus B ' + json_date

        # write the updated JSON to a new Cloud Storage bucket
        archive_bucket = storage_client.get_bucket(os.getenv('ARCHIVE_BUCKET'))
        print(f'archive bucket: {archive_bucket}')
        archive_blob = archive_bucket.blob(dataset_id + '.json')  # used a new variable name
        archive_blob.upload_from_string(json.dumps(json_template, indent=4))  # used the new variable

        # Convert dictionary to JSON string and print it
        #json_string = json.dumps(json_template, indent=4)
        #print(json_string)


    except Forbidden as e:
        print(f'Forbidden error occurred: {str(e)}. Please check the Cloud Function has necessary permissions.')
    except BadRequest as e:
        print(f'Bad request error occurred: {str(e)}. Please check the query and the table.')
    except GoogleAPICallError as e:
        print(f'Google API Call error occurred: {str(e)}. Please check the API request.')
    except Exception as e:
        print(f'An unexpected error occurred: {str(e)}')
