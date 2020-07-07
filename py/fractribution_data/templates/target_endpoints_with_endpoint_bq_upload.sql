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

# Select uploaded customers that converted within a given time period of a Google Analytics session.
#
# Args:
#  target_customer_id_map_matches_table: BigQuery table with matches of uploaded customers against
#    the core_fullvisitorid_customer_id_map_table.
#  max_map_minutes: Restrict to customers with an endpoint time within this many minutes of a
#    Google Analytics session start time.
WITH
  MostRecentMatch AS (
    SELECT
      customer_id,
      endpoint_datetime,
      MIN(abs_time_from_map) AS min_abs_time_from_map
    FROM `{{target_customer_id_map_matches_table}}`
    GROUP BY customer_id, endpoint_datetime
  )
SELECT Tcmm.*
FROM `{{target_customer_id_map_matches_table}}` AS Tcmm
INNER JOIN MostRecentMatch AS Mrm
  ON
    Tcmm.customer_id = Mrm.customer_id
    AND Tcmm.endpoint_datetime = Mrm.endpoint_datetime
    AND Tcmm.abs_time_from_map = Mrm.min_abs_time_from_map
WHERE Tcmm.abs_time_from_map <= {{max_map_minutes}} * 60
