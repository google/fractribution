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

# Gets the total number of conversions, non-conversions and revenue by path.
-- Args:
--  paths_to_conversion_table: BigQuery table described in paths_to_conversion.sql
--  paths_to_non_conversion_table: BigQuery table described in paths_to_non_conversion.sql
WITH PathsToConversion AS (
  SELECT transformedPath, COUNT(*) AS conversions, SUM(revenue) AS revenue
  FROM `{{paths_to_conversion_table}}`
  GROUP BY transformedPath
), PathsToNonConversion AS (
  SELECT transformedPath, COUNT(*) AS nonConversions
  FROM `{{paths_to_non_conversion_table}}` GROUP BY transformedPath
)
SELECT
  IFNULL(PathsToConversion.transformedPath,
         PathsToNonConversion.transformedPath) AS transformedPath,
  IFNULL(PathsToConversion.conversions, 0) AS conversions,
  IFNULL(PathsToNonConversion.nonConversions, 0) AS nonConversions,
  PathsToConversion.revenue
FROM PathsToConversion
FULL JOIN PathsToNonConversion
  USING(transformedPath)
-- Do not include a trailing ; as this query is included in another SQL query.
