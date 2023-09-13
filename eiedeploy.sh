#!/bin/bash

# Source the .env file
source eiedeploy.env

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

