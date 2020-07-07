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

# Augments the fullVistorId to customer_id mapping with from the Google Analytics BigQuery table.
#
# Args:
#  suffix_filter_end: %Y%m%d table decorator end date filter on the ga_sessions_table.
#  ga_sessions_table: Google Analytics BigQuery table.
#  core_fullvisitorid_customer_id_map_table: Existing mapping between fullVisitorId and customer_id.
#  id_ga_custom_dimension_id: Index of the customer_id in the custom dimension.
#  id_regex_filter: SQL clause to filter ids.
WITH
  LatestCustomerIdMapping AS (
    SELECT DISTINCT
      fullVisitorId,
      Hc.value AS customer_id,
      visitStartTime AS mapping_visitStartTime,
      '{{suffix_filter_end}}' AS added_at_end_suffix
    FROM `{{ga_sessions_table}}` AS Ga,
      UNNEST(Ga.hits) AS Hits,
      UNNEST(Hits.customDimensions) AS Hc
    WHERE
      _TABLE_SUFFIX <= '{{suffix_filter_end}}'
      AND (
        (SELECT COUNT(*) FROM `{{core_fullvisitorid_customer_id_map_table}}`) = 0
        OR _TABLE_SUFFIX > (
          SELECT MAX(added_at_end_suffix) FROM `{{core_fullvisitorid_customer_id_map_table}}`)
      )
      AND Hc.index = {{id_ga_custom_dimension_id}}
      {% if id_regex_filter %}
      AND REGEXP_CONTAINS(Hc.value, r'{{id_regex_filter}}')
      {% endif %}
  )
SELECT * FROM `{{core_fullvisitorid_customer_id_map_table}}`
UNION ALL
SELECT * FROM LatestCustomerIdMapping
