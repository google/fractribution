# Steps to deploy a cloud function that runs Fractribution modeling.

Use the following command to deploy:

gcloud functions deploy FractributionModel \
--entry-point main --runtime python37 --trigger-resource FractributionModel \
--trigger-event google.pubsub.topic.publish --timeout 540s \
--project [PROJECT_ID] --memory 2GB

Use the following command to test:

gcloud pubsub topics publish FractributionModel --message '{
"project_id":"[PROJECT_ID]",
"paths_to_conversion_table":"[PROJECT_ID].[DATASET].paths_to_conversion_table",
"paths_to_non_conversion_table":"[PROJECT_ID].[DATASET].paths_to_non_conversion_table",
"path_summary_table":"[PROJECT_ID].[DATASET].path_summary_table",
"report_window_start":"[YYYY-MM-DD]", "report_window_end":"[YYYY-MM-DD]",
"key_on_fullvisitorid":"True",
"core_fullvisitorid_customer_id_map_table":"[PROJECT_ID].[DATASET].core_fullvisitorid_customer_id_map_table",
"path_transform_method":"frequency",
"report_table":"[PROJECT_ID].[DATASET].report"}'

If using the 'Testing' tab in the Cloud function, you will need to base64 encode
the text within the braces {}, and then use the following for the triggering
event text: {"data":"'<base64_encoded_args>'"
