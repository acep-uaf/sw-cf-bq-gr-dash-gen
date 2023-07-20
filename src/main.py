from google.api_core.exceptions import Forbidden, BadRequest, GoogleAPICallError
from google.cloud import pubsub_v1, storage
import logging
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

        if not all([project_id, dataset_id, table_id]):
            print('Error: Missing necessary parameters in the message.')
            return

        #payload = f'{project_id}.{dataset_id}.{table_id}'

        print(f'project id: {project_id}')
        print(f'dataset id: {dataset_id}')
        print(f'table id: {table_id}')

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

        # write the updated JSON to a new Cloud Storage bucket
        archive_bucket = storage_client.get_bucket(os.getenv('ARCHIVE_BUCKET'))
        print(f'archive bucket: {archive_bucket}')
        archive_blob = archive_bucket.blob(dataset_id + '.json')  # used a new variable name
        archive_blob.upload_from_string(json.dumps(json_template))  # used the new variable

        # Convert dictionary to JSON string and print it
        json_string = json.dumps(json_template, indent=4)
        print(json_string)


    except Forbidden as e:
        print(f'Forbidden error occurred: {str(e)}. Please check the Cloud Function has necessary permissions.')
    except BadRequest as e:
        print(f'Bad request error occurred: {str(e)}. Please check the query and the table.')
    except GoogleAPICallError as e:
        print(f'Google API Call error occurred: {str(e)}. Please check the API request.')
    except Exception as e:
        print(f'An unexpected error occurred: {str(e)}')
