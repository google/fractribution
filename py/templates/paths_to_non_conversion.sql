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

# Extracts marketing channel paths for customers that have not converted.
-- Args:
--  path_lookback_days: Restrict to marketing channels within this many days of the conversion.
--  path_lookback_steps: Limit the number of marketing channels before the conversion.
--  path_transform: Function name for transforming the path
--    (e.g. unique, exposure, first, frequency).
WITH Conversions AS (
  SELECT DISTINCT customerId
  FROM ConversionsByCustomerId
),
NonConversions AS (
  SELECT
    SessionsByCustomerId.customerId,
    MAX(visitStartTimestamp) AS nonConversionTimestamp
  FROM SessionsByCustomerId
  LEFT JOIN Conversions
    USING (customerId)
  WHERE Conversions.customerId IS NULL
  GROUP BY SessionsByCustomerId.customerId
)
SELECT
  NonConversions.customerId,
  ARRAY_TO_STRING(TrimLongPath(
    ARRAY_AGG(channel ORDER BY visitStartTimestamp), {{path_lookback_steps}}), ' > ') AS path,
  ARRAY_TO_STRING(
    {% for path_transform_name, _ in path_transforms|reverse %}
      {{path_transform_name}}(
    {% endfor %}
        ARRAY_AGG(channel ORDER BY visitStartTimestamp)
    {% for _, arg_str in path_transforms %}
      {% if arg_str %}, {{arg_str}}{% endif %})
    {% endfor %}
    , ' > ') AS transformedPath,
FROM NonConversions
LEFT JOIN SessionsByCustomerId
  ON
    NonConversions.customerId = SessionsByCustomerId.customerId
    AND TIMESTAMP_DIFF(nonConversionTimestamp, visitStartTimestamp, DAY)
      BETWEEN 0 AND {{path_lookback_days}}
GROUP BY NonConversions.customerId
-- Do not include a trailing ; as this query is included in another SQL query.
