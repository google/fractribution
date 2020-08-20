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

# Extracts new mappings between fullVistorId and userId from the Google Analytics BigQuery table.
-- Args:
--  ga_sessions_table: Google Analytics BigQuery table.
--  userid_ga_custom_dimension_index: Index of the userId in the Google Analytics custom dimension.
--
-- If your fullVisitorId to userId mappings are outside Google Analytics, replace this script with
-- one that queries your table and extracts the following:
--   fullVisitorId STRING NOT NULL,
--   userId STRING NOT NULL,
--   mapStartTimestamp TIMESTAMP NOT NULL,
--   tableSuffixWhenAdded STRING
--     For efficiency, from run to run, only query newer mappings in your table, instead of the
--     entire table.
SELECT DISTINCT
  fullVisitorId,
  IFNULL(userId, CustomDimension.value) AS userId,
  TIMESTAMP_SECONDS(visitStartTime) AS mapStartTimestamp,
  FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)) AS tableSuffixWhenAdded
FROM `{{ga_sessions_table}}` AS Sessions,
  UNNEST(Sessions.hits) AS hits,
  UNNEST(hits.customDimensions) AS customDimension
WHERE
  _TABLE_SUFFIX BETWEEN
    MAX(added_at_end_suffix)
    AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY))
  AND (
    Sessions.userId IS NOT NULL
    {% if userid_ga_custom_dimension_index > 0 %}
    OR (
      CustomDimension.index = {{userid_ga_custom_dimension_index}}
      AND CustomDimension.value IS NOT NULL
    )
    {% endif %}
  )

