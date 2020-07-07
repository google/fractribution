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

# Here we search for sessions starting from {lookback_days} before the
# {report_window_start}, to give enough time for a counterfactual to be
# defined for a conversion at the very start of the reporting period
# Because we have the GA sessions partitioned in local time but the
# visitStartTime in UTC (and we are actually using UTC for our reporting), we
# extend the start and end of our GA sessions table suffix range to the
# previous and next day respectively so that we catch the remaining UTC
# sessions that still fall within the UTC (but not local) reporting period
#
# Args:
#  report_window_start
#  report_window_end
#  visit_filter_start
#  suffix_filter_start
#  suffix_filter_end
#  channel_defn
#  ga_sessions_table
#  hostnames
#  additional_removals_method
#  additional_removals_table
#  core_fullvisitorid_customer_id_map_table: BigQuery table with mapping between
#    fullVisitorId and customer_id
#  target_upload_table
{% if additional_removals_method == 'upload_customer_ids' %}
WITH
  AdditionalVisitorRemovals AS (
    SELECT DISTINCT Cfcm.fullVisitorId AS fullVisitorId
    FROM `{{additional_removals_table}}` AS Ar
    INNER JOIN `{{core_fullvisitorid_customer_id_map_table}}` AS Cfcm
      USING (customer_id)
    WHERE Ar.reference_date BETWEEN '{{report_window_start}}' AND '{{report_window_end}}'
  )
{% elif additional_removals_method == 'previously_known_customer_ids' %}
WITH
  AdditionalVisitorRemovals AS (
    SELECT DISTINCT fullVisitorId
    FROM `{{core_fullvisitorid_customer_id_map_table}}`
    WHERE mapping_visitStartTime < unix_seconds('{{report_window_start}}'))
  )
{% endif %}
SELECT
  fullVisitorId,
  Ga.visitStartTime,
  {{channel_defn}},
  trafficSource.referralPath,
  trafficSource.campaign,
  trafficSource.source,
  trafficSource.medium,
  trafficSource.keyword,
  totals.hits AS total_hits_in_session,
  totals.timeOnSite AS total_time_in_session,
  Hits.page.pagePath AS first_page_path_in_session,
  Hits.page.hostname
FROM
  {% if additional_removals_method in ('upload_customer_ids', 'previously_known_customer_ids') %}
  `{{ga_sessions_table}}` AS Ga
  LEFT JOIN AdditionalVisitorRemovals AS Rem
    USING(fullVisitorId),
  {% else %}
  `{{ga_sessions_table}}` AS Ga,
  {% endif %}
  UNNEST(hits) AS Hits
WHERE
  _TABLE_SUFFIX BETWEEN '{{suffix_filter_start}}' AND '{{suffix_filter_end}}'
  AND Hits.hitNumber = 1
  AND Ga.visitStartTime BETWEEN
    UNIX_SECONDS('{{visit_filter_start}} 00:00:00 UTC')
    AND UNIX_SECONDS('{{report_window_end}} 23:59:59 UTC')
  {% if additional_removals_method in ('upload_customer_ids', 'previously_known_customer_ids') %}
  AND Rem.fullVisitorId IS NULL
  {% endif %}
  {% if hostnames %}
  AND Hits.page.hostname IN ({{hostnames}})
  {% endif %}
