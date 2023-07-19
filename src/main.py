from google.api_core.exceptions import Forbidden, BadRequest, GoogleAPICallError
from google.cloud import pubsub_v1, storage
import logging
import base64
import json
import os

def mk_gr_dsh(event, context):
    print(f'Received event: {event}')  
    logging.info(f'Received event: {event}')

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
        logging.info(f'project id: {project_id}')
        print(f'dataset id: {dataset_id}')
        logging.info(f'dataset id: {dataset_id}')
        print(f'table id: {table_id}')
        logging.info(f'table id: {table_id}')

        # get the JSON template file
        storage_client = storage.Client()
        template_bucket = storage_client.get_bucket(os.getenv('TEMPLATE_BUCKET'))

        print(f'template bucket: {template_bucket}')
        logging.info(f'template bucket: {template_bucket}')


    except Forbidden as e:
        print(f'Forbidden error occurred: {str(e)}. Please check the Cloud Function has necessary permissions.')
    except BadRequest as e:
        print(f'Bad request error occurred: {str(e)}. Please check the query and the table.')
    except GoogleAPICallError as e:
        print(f'Google API Call error occurred: {str(e)}. Please check the API request.')
    except Exception as e:
        print(f'An unexpected error occurred: {str(e)}')
