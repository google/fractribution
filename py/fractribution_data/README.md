# Steps to deploy a cloud function to setup the Fractribution data tables.

Use the following command to deploy:

gcloud functions deploy FractributionData \
--entry-point prepare_input_for_fractribution_custom_endpoint \
--runtime python37 --trigger-resource FractributionData \
--trigger-event google.pubsub.topic.publish --timeout 540s \
--project [PROJECT_ID] --memory 2GB

Use the following command to test:

gcloud pubsub topics publish FractributionData --message '{
"project_id":"[PROJECT_ID]",
"ga_sessions_table":"[CLIENT_GA_TABLE_PATH].ga_sessions_*",
"session_event_log_table":"[PROJECT_ID].[DATASET].session_event_log_table",
"target_upload_table":"[PROJECT_ID].[DATASET].target_upload_table",
"target_customer_id_map_matches_table":"[PROJECT_ID].[DATASET].target_customer_id_map_matches_table",
"target_endpoints_table":"[PROJECT_ID].[DATASET].target_endpoints_table",
"paths_to_conversion_table":"[PROJECT_ID].[DATASET].paths_to_conversion_table",
"paths_to_non_conversion_table":"[PROJECT_ID].[DATASET].paths_to_non_conversion_table",
"path_summary_table":"[PROJECT_ID].[DATASET].path_summary_table",
"channel_counts_table":"[PROJECT_ID].[DATASET].channel_counts_table",
"channel_definitions":"channel_definitions.sql",
"endpoint_definition":"custom_endpoint_definition_example.sql",
"report_window_start":"[YYYY-MM_DD]", "report_window_end":"[YYYY-MM-DD]",
"lookback_days":"30", "lookback_steps":"0", "hostnames":"",
"top_up_core":"False",
"core_fullvisitorid_customer_id_map_table":"[PROJECT_ID].[DATASET].core_fullvisitorid_customer_id_map_table",
"id_ga_custom_dimension_id":"-1", "additional_removals_method":"none",
"augment_empty_conversion_paths":"False" }'

If you are testing this code with public ga_sessions data. example:
"ga_sessions_table":"bigquery-public-data.google_analytics_sample.ga_sessions_*",
"report_window_start":"2016-08-01", "Report_window_end":"2017-08-02",

... then the fractribution_model step will fail because it uses partition
tables, and partition tables don't use dates that far in the past. Comment out
the partition table lines for report_table and tmp_report_table in main.py in
the fractribution_model directory before running that test.

## Automating your executions with Cloud Scheduler:

1.  Decide how often you want to run Fractribution.

    CRON_SCHEDULE = Schedule on which Fractribution will be executed in
    cron-unix format.

    Example: 15 1 1 * * - Run every first day of the month at 1:15AM.

2.  Do not include report_window_start and report_window_end in the parameters,
    instead use these parameters to calculate the report window.

    CURRDATE_OFFSET = The number days offset from the current date.
    REPORT_WINDOW_LENGTH = The number of days in your report period.

    Example:

    -   If CURRDATE_OFFSET = 0 and REPORT_WINDOW_LENGTH = 7, report_window_end =
        Current Date and report_window_start = (report_window_end - 7 days)
    -   If CURRDATE_OFFSET = 2 and REPORT_WINDOW_LENGTH = 30,
        report_window_end = (Current Date - 2 day) and report_window_start =
        (report_window_end - 30 days)

3.  Trigger the FractributionModel after FractributionData is done by including
    the FractributionModel_TOPIC_NAME in the parameters.

4.  Create a cron job to run Fractribution using Cloud Scheduler.

    gcloud scheduler jobs create pubsub Fractribution --schedule
    "[CRON_SCHEDULE]" --topic FractributionData --message-body '{
    "project_id":"[PROJECT_ID]",
    "ga_sessions_table":"[CLIENT_GA_TABLE_PATH].ga_sessions_*",
    "session_event_log_table":"[PROJECT_ID].[DATASET].session_event_log_table",
    "target_upload_table":"[PROJECT_ID].[DATASET].target_upload_table",
    "target_customer_id_map_matches_table":"[PROJECT_ID].[DATASET].target_customer_id_map_matches_table",
    "target_endpoints_table":"[PROJECT_ID].[DATASET].target_endpoints_table",
    "paths_to_conversion_table":"[PROJECT_ID].[DATASET].paths_to_conversion_table",
    "paths_to_non_conversion_table":"[PROJECT_ID].[DATASET].paths_to_non_conversion_table",
    "path_summary_table":"[PROJECT_ID].[DATASET].path_summary_table",
    "channel_counts_table":"[PROJECT_ID].[DATASET].channel_counts_table",
    "channel_definitions":"channel_definitions.sql",
    "endpoint_definition":"custom_endpoint_definition_example.sql",
    "lookback_days":"30", "lookback_steps":"0", "hostnames":"",
    "top_up_core":"False",
    "core_fullvisitorid_customer_id_map_table":"[PROJECT_ID].[DATASET].core_fullvisitorid_customer_id_map_table",
    "id_ga_custom_dimension_id":"-1", "additional_removals_method":"none",
    "augment_empty_conversion_paths":"False",
    "report_window_length":"[REPORT_WINDOW_LENGTH]",
    "report_window_end_offset_from_currdate":"[CURRDATE_OFFSET]",
    "model_topic_name":"[FractributionModel_TOPIC_NAME]",
    "key_on_fullvisitorid":"True", "path_transform_method":"frequency",
    "report_table":"[PROJECT_ID].[DATASET].report"}'
