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

# Extracts marketing channel paths that end in conversion endpoints for the customer.
#
# Args:
#  target_endpoints_table: BigQuery table described in
#      target_endpoints_with_endpoint_definition.sql and
#      target_endpoints_with_endpoint_bq_upload.sql
#  session_event_log_table: BigQuery table described in session_event_log.sql
#  lookback_days: Only include marketing events that are within this many days of the conversion.
#  lookback_steps: Limit paths to at most this many marketing channel events.
WITH
  PathsToConversion AS (
    SELECT
      Conv.fullVisitorId,
      Conv.customer_id,
      Conv.endpoint_datetime,
      Conv.endpoint_datetime_utc,
      STRUCT(
        S.fullVisitorId,
        S.channel,
        S.visitStartTime,
        ROW_NUMBER()
          OVER (PARTITION BY Conv.fullVisitorId, Conv.endpoint_datetime ORDER BY S.visitStartTime)
          AS pathRank,
        ROW_NUMBER()
          OVER (
            PARTITION BY Conv.fullVisitorId, Conv.endpoint_datetime
            ORDER BY S.visitStartTime DESC
          ) AS pathRecency,
        {% if lookback_steps <= 0 %}
        STRING_AGG(channel, ' > ')
          OVER (PARTITION BY Conv.fullVisitorId, Conv.endpoint_datetime ORDER BY S.visitStartTime)
          AS path,
        {% endif %}
        Conv.endpoint_datetime - S.visitStartTime AS seconds_before_endpoint,
        CAST((Conv.endpoint_datetime - S.visitStartTime) / (24 * 60 * 60) AS INT64)
          AS days_before_endpoint
      ) AS sessions
    FROM `{{target_endpoints_table}}` AS Conv
    INNER JOIN `{{session_event_log_table}}` AS S
      ON
        Conv.fullVisitorId = S.fullVisitorId
        AND Conv.endpoint_datetime >= S.visitStartTime
        AND (Conv.endpoint_datetime - S.visitStartTime) <= ({{lookback_days}} * 24 * 60 * 60)
  {% if lookback_steps > 0 %}
  ),
  PathsToConversionWithLookbackSteps AS (
    SELECT
      fullVisitorId,
      customer_id,
      endpoint_datetime,
      endpoint_datetime_utc,
      STRUCT(
        sessions.fullVisitorId,
        sessions.channel,
        sessions.visitStartTime,
        sessions.pathRank,
        sessions.pathRecency,
        STRING_AGG(sessions.channel, ' > ')
          OVER (PARTITION BY fullVisitorId, endpoint_datetime ORDER BY sessions.visitStartTime)
          AS path,
        sessions.seconds_before_endpoint,
        sessions.days_before_endpoint
      ) AS sessions
    FROM PathsToConversion
    WHERE sessions.pathRecency <= {{lookback_steps}}
  {% endif %}
  {% if augment_empty_conversion_paths %}
  ),
  PathsToConversionWithNulls AS (
    SELECT
      fullVisitorId,
      customer_id,
      endpoint_datetime,
      endpoint_datetime_utc,
      ARRAY_AGG(sessions ORDER BY sessions.visitStartTime) AS sessions
    FROM
      {% if lookback_steps <= 0 %} PathsToConversion
      {% else %} PathsToConversionWithLookbackSteps
      {% endif %}
    GROUP BY 1,2,3,4
  ),
  PathlessConverters AS (
    SELECT
      Pc.fullVisitorId,
      Pc.customer_id,
      Pc.endpoint_datetime,
      Pc.endpoint_datetime_utc
    FROM PathsToConversionWithNulls AS Pc, UNNEST(sessions) AS S
    WHERE S.channel IS NULL
  ),
  PostConversionEvents AS (
    SELECT
      Pc.*,
      S.channel,
      S.visitStartTime,
      ROW_NUMBER()
        OVER (PARTITION BY Pc.fullVisitorId, Pc.endpoint_datetime ORDER BY S.visitStartTime)
        AS channel_order
    FROM PathlessConverters AS Pc
    JOIN `{{session_event_log_table}}` AS S USING (fullVisitorId)
    WHERE S.visitStartTime > Pc.endpoint_datetime
  ),
  ReplacementPaths AS (
    SELECT
      fullVisitorId,
      customer_id,
      endpoint_datetime,
      endpoint_datetime_utc,
      STRUCT(
        fullVisitorId,
        channel,
        visitStartTime,
        1 AS pathRank,
        1 AS pathRecency,
        channel AS path,
        0 AS seconds_before_endpoint,
        0 AS days_before_endpoint
      ) AS sessions
    FROM PostConversionEvents
    WHERE channel_order = 1
  ),
  ReplacementPathsAgg AS (
    SELECT
      fullVisitorId,
      customer_id,
      endpoint_datetime,
      endpoint_datetime_utc,
      ARRAY_AGG(sessions ORDER BY sessions.visitStartTime) AS sessions
    FROM ReplacementPaths
    GROUP BY 1,2,3,4
  ),
  PathsToConversionWithoutNulls AS (
    SELECT
      Pc.fullVisitorId,
      Pc.customer_id,
      Pc.endpoint_datetime,
      Pc.endpoint_datetime_utc,
      STRUCT(
        S.fullVisitorId,
        S.channel,
        S.visitStartTime,
        S.pathRank,
        S.pathRecency,
        S.path,
        S.seconds_before_endpoint,
        S.days_before_endpoint
      ) AS sessions
    FROM PathsToConversionWithNulls AS Pc, UNNEST(sessions) AS S
    WHERE S.channel IS NOT NULL
  {% endif %}
  )
{% if not augment_empty_conversion_paths %}
SELECT
  fullVisitorId,
  endpoint_datetime,
  endpoint_datetime_utc,
  ARRAY_AGG(sessions ORDER BY sessions.visitStartTime) AS sessions
FROM
  {% if lookback_steps <= 0 %} PathsToConversion
  {% else %} PathsToConversionWithLookbackSteps
  {% endif %}
GROUP BY 1,2,3
{% else %}
SELECT
  fullVisitorId,
  customer_id,
  endpoint_datetime,
  endpoint_datetime_utc,
  ARRAY_AGG(sessions ORDER BY sessions.visitStartTime) AS sessions
FROM PathsToConversionWithoutNulls
GROUP BY 1,2,3,4
UNION ALL
SELECT * FROM ReplacementPathsAgg
{% endif %}
