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

# Extracts new mappings between fullVistorId and userId from the Google Analytics BigQuery table.
-- Args:
--  ga_sessions_table: Google Analytics BigQuery table.
--  userid_ga_custom_dimension_index: Index of the userId in the Google Analytics custom dimension.
--  userid_ga_hits_custom_dimension_index: Index of the userId in the Google Analytics hits custom
--    dimension.
--  fullvisitorid_userid_map_table: BigQuery table with mappings from fullVisitorIds to userIds.
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
  CASE
    WHEN userId IS NOT NULL THEN userId
    {% if userid_ga_custom_dimension_index > 0 %}
    WHEN
      customDimension.index = {{userid_ga_custom_dimension_index}}
      AND customDimension.value IS NOT NULL
      THEN customDimension.value
    {% endif %}
    {% if userid_ga_hits_custom_dimension_index > 0 %}
    WHEN
      hitsCustomDimension.index = {{userid_ga_hits_custom_dimension_index}}
      AND hitsCustomDimension.value IS NOT NULL
      THEN hitsCustomDimension.value
    {% endif %}
    ELSE NULL
    END AS userId,
  TIMESTAMP_SECONDS(visitStartTime) AS mapStartTimestamp,
  _TABLE_SUFFIX AS tableSuffixWhenAdded
FROM `{{ga_sessions_table}}` AS Sessions
  {% if userid_ga_custom_dimension_index > 0 %}
  LEFT JOIN UNNEST(Sessions.customDimensions) as customDimension
  {% endif %}
  {% if userid_ga_hits_custom_dimension_index > 0 %},
  UNNEST(Sessions.hits) as hits  -- hits cannot be empty, since Sessions begin with a hit.
  LEFT JOIN UNNEST(hits.customDimensions) as hitsCustomDimension
  {% endif %}
WHERE
  _TABLE_SUFFIX BETWEEN
    -- From one day after the maximum table suffix previously recorded.
    (SELECT
       FORMAT_DATE(
         "%Y%m%d",
         DATE_ADD(PARSE_DATE("%Y%m%d", IFNULL(MAX(tableSuffixWhenAdded), "19700101")),
                  INTERVAL 1 DAY))  -- 1 day after the latest tableSuffixWhenAdded.
     FROM `{{fullvisitorid_userid_map_table}}`)
    -- To yesterday.
    AND FORMAT_DATE('%Y%m%d', CURRENT_DATE('UTC') - 1)
  AND fullVisitorId IS NOT NULL
  AND (
    Sessions.userId IS NOT NULL
    AND LOWER(Sessions.userId) NOT IN ('', 'undefined', 'n/a')
    {% if userid_ga_custom_dimension_index > 0 %}
    OR (
      customDimension.index = {{userid_ga_custom_dimension_index}}
      AND customDimension.value IS NOT NULL
      AND LOWER(customDimension.value) NOT IN ('', 'undefined', 'n/a')
    )
    {% endif %}
    {% if userid_ga_hits_custom_dimension_index > 0 %}
    OR (
      hitsCustomDimension.index = {{userid_ga_hits_custom_dimension_index}}
      AND hitsCustomDimension.value IS NOT NULL
      AND LOWER(hitsCustomDimension.value) NOT IN ('', 'undefined', 'n/a')
    )
    {% endif %}
  )

