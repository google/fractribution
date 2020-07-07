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

# Extracts marketing channel paths that do not in conversion for the customer.
#
# Args:
#  converted_customers_table: BigQuery table described in paths_to_conversion.sql
#  session_event_log_table: BigQuery table described in session_event_log.sql
#  lookback_days
#  lookback_steps
WITH
  ConversionVisitors AS (
    SELECT DISTINCT fullVisitorId
    FROM `{{converted_customers_table}}`
  ),
  NonConversionEndpoints AS (
    SELECT
      s.fullVisitorId,
      MAX(s.visitStartTime) AS path_end_time
    FROM `{{session_event_log_table}}` AS s
    LEFT JOIN ConversionVisitors AS rem
      USING (fullVisitorId)
    WHERE rem.fullVisitorId IS NULL
    GROUP BY fullVisitorId
  ),
  NonConversionPaths AS (
    SELECT
      ep.fullVisitorId,
      STRUCT(
        s.fullVisitorId,
        s.channel,
        s.visitStartTime,
        ROW_NUMBER() OVER (PARTITION BY ep.fullVisitorId ORDER BY s.visitStartTime) AS pathRank,
        ROW_NUMBER()
          OVER (PARTITION BY ep.fullVisitorId ORDER BY s.visitStartTime DESC)
          AS pathRecency,
        {% if lookback_steps <= 0 %}
        STRING_AGG(channel, ' > ')
          OVER (PARTITION BY ep.fullVisitorId ORDER BY s.visitStartTime)
          AS path,
        {% endif %}
        CAST((ep.path_end_time - s.visitstartTime) / (24 * 60 * 60) AS INT64)
          AS days_before_end_point
      ) AS sessions
    FROM NonConversionEndpoints AS ep
    LEFT JOIN `{{session_event_log_table}}` AS s
      ON
        ep.fullVisitorId = s.fullVisitorId
        AND ep.path_end_time >= s.visitStartTime
        AND (ep.path_end_time - s.visitStartTime) <= ({{lookback_days}} * 24 * 60 * 60)
  {% if lookback_steps > 0 %}
  ),
  NonConversionPathsLookbackSteps AS (
    SELECT
      fullVisitorId,
      STRUCT(
        sessions.fullVisitorId,
        sessions.channel,
        sessions.visitStartTime,
        sessions.pathRank,
        sessions.pathRecency,
        STRING_AGG(sessions.channel, ' > ')
          OVER (PARTITION BY fullVisitorId ORDER BY sessions.visitStartTime)
          AS path
      ) AS sessions
    FROM NonConversionPaths
    WHERE sessions.pathRecency <= {{lookback_steps}}
  {% endif %}
  )
SELECT
  fullVisitorId,
  ARRAY_AGG(sessions ORDER BY sessions.visitStartTime) AS sessions
FROM
  {% if lookback_steps <= 0 %} NonConversionPaths
  {% else %} NonConversionPathsLookbackSteps
  {% endif %}
GROUP BY fullVisitorId
