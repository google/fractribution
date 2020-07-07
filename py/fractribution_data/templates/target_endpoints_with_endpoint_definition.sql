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

# Extract converted customers from Google Analytics with a given SQL endpoint/conversion definition.
#
# Args:
#  ga_sessions_table: Google Analytics BigQuery table.
#  suffix_filter_start: %Y%m%d table decorator start date filter.
#  suffix_filter_end: %Y%m%d table decorator end date filter.
#  visit_filter_start: %Y-%m-%d date string. Ignore conversions before this date.
#  visit_filter_end: %Y-%m-%d date string. Ignore conversions after this date.
#  endpoint_definition: Custom SQL that defines the conversion/endpoint.
SELECT
  Ga.fullVisitorId,
  Ga.fullVisitorId AS customer_id,
  Ga.visitStartTime,
  MIN(SAFE_CAST(Ga.visitStartTime + (Hits.time / 1e3) AS INT64)) AS endpoint_datetime,
  FORMAT_TIMESTAMP(
      '%Y-%m-%d %H:%M:%S',
      TIMESTAMP_SECONDS(MIN(SAFE_CAST(Ga.visitStartTime + (Hits.time / 1e3) AS INT64)))
  ) AS endpoint_datetime_utc,
  {% if 'Hits.eventInfo' in endpoint_definition %}
  Hits.eventInfo.eventCategory AS event_category,
  Hits.eventInfo.eventAction AS event_action,
  {% endif %}
  Hits.page.pagePath AS page_path,
  Hits.page.hostname AS hostname
FROM
  `{{ga_sessions_table}}` AS Ga,
  UNNEST(Ga.hits) AS Hits{% if 'Hc.' in endpoint_definition %},
  UNNEST(Hits.customDimensions) AS Hc
  {% endif %}
WHERE
  _TABLE_SUFFIX BETWEEN '{{suffix_filter_start}}' AND '{{suffix_filter_end}}'
  AND Ga.visitStartTime BETWEEN
    UNIX_SECONDS('{{visit_filter_start}} 00:00:00 UTC')
    AND UNIX_SECONDS('{{visit_filter_end}} 23:59:59 UTC')
  AND ({{endpoint_definition}})
GROUP BY
  fullVisitorId,
  customer_id,
  visitStartTime,
  {% if 'Hits.eventInfo' in endpoint_definition %}
  event_category,
  event_action,
  {% endif %}
  page_path,
  hostname
