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

# Extracts marketing channel paths that end in conversion for the customer.
-- Args:
--  path_lookback_days: Restrict to marketing channels within this many days of the conversion.
--  path_lookback_steps: Limit the number of marketing channels before the conversion.
--  path_transform: Function name for transforming the path
--    (e.g. unique, exposure, first, frequency).
SELECT
  ConversionsByCustomerId.customerId,
  conversionTimestamp,
  revenue,
  ARRAY_TO_STRING(TrimLongPath(
    ARRAY_AGG(channel ORDER BY visitStartTimestamp), {{path_lookback_steps}}),
    ' > ') AS path,
  ARRAY_TO_STRING(
    {% for path_transform_name, _ in path_transforms|reverse %}
      {{path_transform_name}}(
    {% endfor %}
        ARRAY_AGG(channel ORDER BY visitStartTimestamp)
    {% for _, arg_str in path_transforms %}
      {% if arg_str %}, {{arg_str}}{% endif %})
    {% endfor %}
    , ' > ') AS transformedPath,
FROM ConversionsByCustomerId
LEFT JOIN SessionsByCustomerId
  ON
    ConversionsByCustomerId.customerId = SessionsByCustomerId.customerId
    AND TIMESTAMP_DIFF(conversionTimestamp, visitStartTimestamp, DAY)
      BETWEEN 0 AND {{path_lookback_days}}
GROUP BY
  ConversionsByCustomerId.customerId,
  conversionTimestamp,
  revenue
-- Do not include a trailing ; as this query is included in another SQL query.
