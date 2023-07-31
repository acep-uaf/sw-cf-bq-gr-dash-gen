from google.api_core.exceptions import Forbidden, BadRequest, GoogleAPICallError
from google.cloud import bigquery, storage, pubsub_v1
from datetime import timedelta

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
        WITH time_series AS (
        SELECT TIMESTAMP_TRUNC(t, HOUR) as ts_datetime
        FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
            (SELECT MIN(TIMESTAMP(datetime)) FROM `{bigquery_uri}`), 
            (SELECT MAX(TIMESTAMP(datetime)) FROM `{bigquery_uri}`), 
            INTERVAL 1 HOUR)
        ) t
        ),
        data AS (
        SELECT
            TIMESTAMP_TRUNC(TIMESTAMP(datetime), HOUR) as data_datetime,
            COUNTIF(measurement_name = 'DT1-B_VAB') AS DT1_B_VAB_count,
            COUNTIF(measurement_name = 'DT1-B_VBC') AS DT1_B_VBC_count,
            COUNTIF(measurement_name = 'DT1-B_VCA') AS DT1_B_VCA_count,
            COUNTIF(measurement_name = 'DT2-B_VAB') AS DT2_B_VAB_count,
            COUNTIF(measurement_name = 'DT2-B_VBC') AS DT2_B_VBC_count,
            COUNTIF(measurement_name = 'DT2-B_VCA') AS DT2_B_VCA_count,
            COUNTIF(measurement_name = 'DT3-B_VAB') AS DT3_B_VAB_count,
            COUNTIF(measurement_name = 'DT3-B_VBC') AS DT3_B_VBC_count,
            COUNTIF(measurement_name = 'DT3-B_VCA') AS DT3_B_VCA_count,
            COUNTIF(measurement_name = 'GVEA-B_VAB') AS GVEA_B_VAB_count,
            COUNTIF(measurement_name = 'GVEA-B_VBC') AS GVEA_B_VBC_count,
            COUNTIF(measurement_name = 'GVEA-B_VCA') AS GVEA_B_VCA_count,
            COUNTIF(measurement_name = 'G3-B_VAB') AS G3_B_VAB_count,
            COUNTIF(measurement_name = 'G3-B_VBC') AS G3_B_VBC_count,
            COUNTIF(measurement_name = 'G3-B_VCA') AS G3_B_VCA_count,
            COUNTIF(measurement_name = 'G5-B_VAB') AS G5_B_VAB_count,
            COUNTIF(measurement_name = 'G5-B_VBC') AS G5_B_VBC_count,
            COUNTIF(measurement_name = 'G5-B_VCA') AS G5_B_VCA_count,
            COUNTIF(measurement_name = 'GVEA-B_Frequency') AS GVEA_B_Frequency_count,
            COUNTIF(measurement_name = 'DT1-B_IA') AS DT1_B_IA_count,
            COUNTIF(measurement_name = 'DT1-B_IB') AS DT1_B_IB_count,
            COUNTIF(measurement_name = 'DT1-B_IC') AS DT1_B_IC_count,
            COUNTIF(measurement_name = 'DT2-B_IA') AS DT2_B_IA_count,
            COUNTIF(measurement_name = 'DT2-B_IB') AS DT2_B_IB_count,
            COUNTIF(measurement_name = 'DT2-B_IC') AS DT2_B_IC_count,
            COUNTIF(measurement_name = 'DT3-B_IA') AS DT3_B_IA_count,
            COUNTIF(measurement_name = 'DT3-B_IB') AS DT3_B_IB_count,
            COUNTIF(measurement_name = 'DT3-B_IC') AS DT3_B_IC_count,
            COUNTIF(measurement_name = 'GVEA-B_IA') AS GVEA_B_IA_count,
            COUNTIF(measurement_name = 'GVEA-B_IB') AS GVEA_B_IB_count,
            COUNTIF(measurement_name = 'GVEA-B_IC') AS GVEA_B_IC_count,
            COUNTIF(measurement_name = 'G3-B_IA') AS G3_B_IA_count,
            COUNTIF(measurement_name = 'G3-B_IB') AS G3_B_IB_count,
            COUNTIF(measurement_name = 'G3-B_IC') AS G3_B_IC_count,
            COUNTIF(measurement_name = 'G5-B_IA') AS G5_B_IA_count,
            COUNTIF(measurement_name = 'G5-B_IB') AS G5_B_IB_count,
            COUNTIF(measurement_name = 'G5-B_IC') AS G5_B_IC_count,
            COUNTIF(measurement_name = 'DT1-B_VA_A') AS DT1_B_VA_A_count,
            COUNTIF(measurement_name = 'DT1-B_VA_C') AS DT1_B_VA_C_count,
            COUNTIF(measurement_name = 'DT2-B_VA_A') AS DT2_B_VA_A_count,
            COUNTIF(measurement_name = 'DT2-B_VA_C') AS DT2_B_VA_C_count,
            COUNTIF(measurement_name = 'DT3-B_VA_A') AS DT3_B_VA_A_count,
            COUNTIF(measurement_name = 'DT3-B_VA_C') AS DT3_B_VA_C_count,
            COUNTIF(measurement_name = 'DT1-B_W_Total') AS DT1_B_W_Total_count,
            COUNTIF(measurement_name = 'DT2-B_W_Total') AS DT2_B_W_Total_count,
            COUNTIF(measurement_name = 'DT3-B_W_Total') AS DT3_B_W_Total_count  
        FROM `{bigquery_uri}`
        GROUP BY 1
        )
        SELECT *
        FROM time_series ts
        LEFT JOIN data d
            ON ts.ts_datetime = d.data_datetime
        WHERE DT1_B_VAB_count >= 3000
            AND DT1_B_VBC_count >= 3000
            AND DT1_B_VCA_count >= 3000
            AND DT2_B_VAB_count >= 3000
            AND DT2_B_VBC_count >= 3000
            AND DT2_B_VCA_count >= 3000
            AND DT3_B_VAB_count >= 3000
            AND DT3_B_VBC_count >= 3000
            AND DT3_B_VCA_count >= 3000
            AND GVEA_B_VAB_count >= 3000
            AND GVEA_B_VBC_count >= 3000
            AND GVEA_B_VCA_count >= 3000
            AND G3_B_VAB_count >= 3000
            AND G3_B_VBC_count >= 3000
            AND G3_B_VCA_count >= 3000
            AND G5_B_VAB_count >= 3000
            AND G5_B_VBC_count >= 3000
            AND G5_B_VCA_count >= 3000
            AND GVEA_B_Frequency_count >= 3000
            AND DT1_B_IA_count >= 3000
            AND DT1_B_IB_count >= 3000
            AND DT1_B_IC_count >= 3000
            AND DT2_B_IA_count >= 3000
            AND DT2_B_IB_count >= 3000
            AND DT2_B_IC_count >= 3000
            AND DT3_B_IA_count >= 3000
            AND DT3_B_IB_count >= 3000
            AND DT3_B_IC_count >= 3000
            AND GVEA_B_IA_count >= 3000
            AND GVEA_B_IB_count >= 3000
            AND GVEA_B_IC_count >= 3000
            AND G3_B_IA_count >= 3000
            AND G3_B_IB_count >= 3000
            AND G3_B_IC_count >= 3000
            AND G5_B_IA_count >= 3000
            AND G5_B_IB_count >= 3000
            AND G5_B_IC_count >= 3000
            AND DT1_B_VA_A_count >= 800
            AND DT1_B_VA_C_count >= 800 
            AND DT2_B_VA_A_count >= 800
            AND DT2_B_VA_C_count >=800
            AND DT3_B_VA_A_count >=800
            AND DT3_B_VA_C_count >=800
            AND DT1_B_W_Total_count >=800
            AND DT2_B_W_Total_count >=800
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
            print(f'{row.ts_datetime}')
            
            # If first_ts is None, set it to the current timestamp
            if first_ts is None:
                first_ts = row.ts_datetime

        # Convert first_ts to a string in 'yyyy-mm-ddTHH:MM:SS.000Z' format
        first_ts_str = first_ts.strftime('%Y-%m-%dT%H:%M:%S.000Z')

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
        json_template['time']['from'] = first_ts_str
        json_template['time']['to'] = (first_ts + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        json_template['title'] = 'SW Grid Base Line - VT&D - Bus B ' + json_date

        # write the updated JSON to a new Cloud Storage bucket
        archive_bucket = storage_client.get_bucket(os.getenv('ARCHIVE_BUCKET'))
        print(f'archive bucket: {archive_bucket}')
        archive_blob = archive_bucket.blob(dataset_id + '_SW_Grid_Base_Line_VTND_Bus_B.json')  
        archive_blob.upload_from_string(json.dumps(json_template, indent=4)) 

        # Run the query
        query_job = client.query(query)
        query_job.result()  # Wait for the job to finish

        # Code to publish to PubSub Topic
        pubsub_topic = os.getenv('PUBSUB_TOPIC')
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, pubsub_topic)  # Use the variable here

        message = {
            'project_id': project_id,
        }
        
        publish_message = publisher.publish(topic_path, json.dumps(message).encode('utf-8'))
        publish_message.result() 

    except Forbidden as e:
        print(f'Forbidden error occurred: {str(e)}. Please check the Cloud Function has necessary permissions.')
    except BadRequest as e:
        print(f'Bad request error occurred: {str(e)}. Please check the query and the table.')
    except GoogleAPICallError as e:
        print(f'Google API Call error occurred: {str(e)}. Please check the API request.')
    except Exception as e:
        print(f'An unexpected error occurred: {str(e)}')
