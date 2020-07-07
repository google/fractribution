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

# Extracts the set of customers and their paths to run fractribution over.
#
# Args:
#  key_on_fullvisitorid: True to use Google's fullVisitorId as the customer_id,
#    and False to use the external customer_id.
#  paths_to_conversion_table: Name of the BigQuery table produced by
#    ../data/templates/paths_to_conversion.sql.
#  core_fullvisitorid_customer_id_map_table: Name of the BigQuery table
#    produced by ../data/templates/core_fullvisitorid_customer_id_map_table.sql
#  max_map_minutes: Maximum integer time to consider a match between
#    customer_id and fullVisitorId in core_fullvisitorid_customer_id_map_table.
{% if key_on_fullvisitorid %}
SELECT DISTINCT
  IFNULL(S.path, 'Unmatched Channel') AS path,
  Paths.fullVisitorId AS customer_id,
  Paths.endpoint_datetime
FROM `{{paths_to_conversion_table}}` AS Paths,
UNNEST(Paths.sessions) AS S
WHERE S.pathRecency = 1
ORDER BY endpoint_datetime
{% else %}
WITH PathFullvisitorIdMap AS (
  SELECT DISTINCT
    IFNULL(S.path, 'Unmatched Channel') AS path,
    Paths.fullVisitorId,
    endpoint_datetime
  FROM `{{paths_to_conversion_table}}` AS Paths,
  UNNEST(Paths.sessions) AS S
  WHERE S.pathRecency = 1
),
CustomerIdMatches AS (
  SELECT DISTINCT
    Pfm.path,
    Pfm.fullVisitorId,
    Pfm.endpoint_datetime,
    Cfcm.customer_id,
    abs(Pfm.endpoint_datetime - Cfcm.mapping_visitStartTime) AS abs_time_from_map
  FROM PathFullvisitorIdMap AS Pfm
  JOIN `{{core_fullvisitorid_customer_id_map_table}}` AS Cfcm
  USING(fullVisitorId)
),
MostRecentMatch AS (
  SELECT
    fullVisitorId,
    endpoint_datetime,
    MIN(abs_time_from_map) AS min_abs_time_from_map
  FROM CustomerIdMatches
  GROUP BY 1,2
),
MatchesWithinTime AS (
  SELECT
    Cim.path,
    Cim.customer_id,
    Cim.fullVisitorId,
    Cim.endpoint_datetime
  FROM CustomerIdMatches AS Cim
  INNER JOIN MostRecentMatch AS Mrm
  ON
    Cim.fullVisitorId = Mrm.fullVisitorId
    AND Cim.endpoint_datetime = Mrm.endpoint_datetime
    AND Cim.abs_time_from_map = Mrm.min_abs_time_from_map
  WHERE Cim.abs_time_from_map <= {{max_map_minutes}} * 60
),
NonMatches AS (
  SELECT
    Pfm.path,
    '[**NO CUSTOMER ID FOUND**]' AS customer_id,
    Pfm.fullVisitorId,
    Pfm.endpoint_datetime
  FROM PathFullvisitorIdMap AS Pfm
  LEFT JOIN MatchesWithinTime AS Mwt
  ON Pfm.fullVisitorId = Mwt.fullVisitorId AND Pfm.endpoint_datetime = Mwt.endpoint_datetime
  WHERE Mwt.fullVisitorId IS NULL
){% if add_unmatched_customers_from_table %},
CustomerNoIdMatches AS (
  SELECT DISTINCT
    '' AS path,
    EndpointTable.customer_id,
    UNIX_SECONDS(EndpointTable.endpoint_datetime) AS endpoint_datetime
  FROM `{{add_unmatched_customers_from_table}}` EndpointTable
  LEFT JOIN CustomerIdMatches as Cim USING (customer_id)
  WHERE
    EndpointTable.endpoint_datetime BETWEEN
      '{{report_window_start}} 00:00:00 UTC' AND '{{report_window_end}} 23:59:59 UTC'
    AND Cim.customer_id IS NULL
)
{% endif %}
SELECT * EXCEPT(fullVisitorId) FROM MatchesWithinTime
UNION ALL
SELECT * EXCEPT(fullVisitorId) FROM NonMatches
{% if add_unmatched_customers_from_table %}
UNION ALL
SELECT * FROM CustomerNoIdMatches
{% endif %}
ORDER BY endpoint_datetime
{% endif %}
