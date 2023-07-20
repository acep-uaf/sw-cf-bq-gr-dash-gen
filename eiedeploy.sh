gcloud functions deploy sw-cw-bq-gr-dash-gen \
  --gen2 \
  --runtime=python311 \
  --region=us-west1 \
  --source=src \
  --entry-point=mk_gr_dsh \
  --memory 16384MB \
  --timeout 540s  \
  --trigger-topic sw-cf-bq-pp-dt-rs \
  --set-env-vars TEMPLATE_BUCKET=sw-eielson-dash-template,TEMPLATE_JSON=sw-bq-grafana-template-b.json,ARCHIVE_BUCKET=sw-eielson-dash-archive