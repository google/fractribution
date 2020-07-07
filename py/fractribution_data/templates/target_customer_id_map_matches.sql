# coding=utf-8
# Copyright 2020 Google LLC..
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Matches uploaded customers against the core_fullvisitorid_customer_id_map_table.
#
# Args:
#  endpoint_datetime: The conversion endpoint datetime in UTC.
#  endpoint_upload_bq_table: BigQuery table with customer_id and conversion endpoint_datetime
#    timestamp
#  report_window_start: %Y-%m-%d string representing the start of the reporting window.
#  report_window_end: %Y-%m-%d string representing the end of the reporting window.
#  core_fullvisitorid_customer_id_map_table: BigQuery table with mapping between
#    fullVisitorId and customer_id

WITH
  TargetUploads AS (
    SELECT
      customer_id,
      unix_seconds(endpoint_datetime) AS endpoint_datetime
    FROM `{{endpoint_upload_bq_table}}`
    WHERE
      endpoint_datetime BETWEEN
        '{{report_window_start}} 00:00:00 UTC' AND '{{report_window_end}} 23:59:59 UTC'
  )
SELECT DISTINCT
  Ep.customer_id,
  Ep.endpoint_datetime,
  Cfcm.fullVisitorId,
  ABS(Ep.endpoint_datetime - Cfcm.mapping_visitStartTime) AS abs_time_from_map
FROM TargetUploads AS Ep
INNER JOIN `{{core_fullvisitorid_customer_id_map_table}}` AS Cfcm
  USING (customer_id)
