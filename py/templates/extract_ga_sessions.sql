# Copyright 2022 Google LLC..
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

# Extracts information from the BigQuery Google Analytics table for constructing paths of channels.
--
-- This script goes through the Google Analytics sessions and pulls out the customerId and
-- traffic-source marketing channel, as defined in channel_definitions.sql. The window for
-- extracting sessions includes the conversion window, plus the preceding {{path_lookback_days}}.
-- Also, the window is extended by one day on either side to account for the sessions being
-- partitioned in local time, while the session.visitStartTime is in UTC (and fractribution
-- reports in UTC).
--
-- Args:
--  ga_sessions_table: Google Analytics BigQuery table.
--  fullvisitorid_userid_map_table: BigQuery table of distinct (fullVisitorId, userId) mappings
--  conversion_window_start_date: Start date of the conversion window in %Y-%m-%d format.
--  conversion_window_end_date: End date of the conversion window in %Y-%m-%d format.
--  channel_definitions_sql: SQL mapping from channel definitions to channel names
--  path_lookback_days: Number of days to extract sessions before the conversion_window_start_date.
--  hostnames: Comma separated list of hostnames to restrict to.
--
-- What about channel touchpoints not recorded in Google Analytics:
--   If you have a third party source of channel touchpoints, add a new SELECT statement below
--   to extact the additional channels touchpoints and then UNION ALL the results with the channels
--   extracted in this script.
WITH
  FilteredSessions AS (
    SELECT
      fullVisitorId,
      TIMESTAMP_SECONDS(visitStartTime) as visitStartTimestamp,
      {% filter indent(width=6) %}
      {{channel_definitions_sql}} AS channel,
      {% endfilter %}
      trafficSource.referralPath,
      trafficSource.campaign,
      trafficSource.source,
      trafficSource.medium
    FROM
      `{{ga_sessions_table}}` AS Sessions
      {% if hostnames %},
      UNNEST(hits) AS hits
      {% endif %}
    WHERE
      _TABLE_SUFFIX
        BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(
          TIMESTAMP('{{conversion_window_start_date}}'), INTERVAL {{path_lookback_days + 1}} DAY))
        AND FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_ADD(
          TIMESTAMP('{{conversion_window_end_date}}'), INTERVAL 1 DAY))
      AND visitStartTime BETWEEN
        UNIX_SECONDS(TIMESTAMP_SUB(
          TIMESTAMP('{{conversion_window_start_date}}'), INTERVAL {{path_lookback_days}}  DAY))
        AND UNIX_SECONDS('{{conversion_window_end_date}} 23:59:59 UTC')
      {% if hostnames %}
      AND hits.hitNumber = 1
      AND hits.page.hostname IN ({{hostnames}})
      {% endif %}
  ),
  FullVisitorIdUserIdMapTable AS (
    SELECT DISTINCT fullVisitorId, userId FROM `{{fullvisitorid_userid_map_table}}`
  )
SELECT
  CASE
    WHEN FullVisitorIdUserIdMapTable.userId IS NOT NULL
      THEN CONCAT('u', FullVisitorIdUserIdMapTable.userId)
    ELSE CONCAT('f', FilteredSessions.fullVisitorId)
  END AS customerId,
  FilteredSessions.* EXCEPT (fullVisitorId)
FROM FilteredSessions
LEFT JOIN FullVisitorIdUserIdMapTable USING (fullVisitorId)
-- Do not include a trailing ; as this query is included in another SQL query.
