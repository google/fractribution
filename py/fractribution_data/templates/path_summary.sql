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

# Queries the total number of conversions and non-conversion by path.
#
# Args:
#  paths_to_conversion_table: BigQuery table described in paths_to_conversion.sql
#  paths_to_non_conversion_table: BigQuery table described in paths_to_non_conversion.sql
WITH
  ConversionPaths AS (
    SELECT
      IFNULL(s.path, 'Unmatched Channel') AS path,
      COUNT(*) AS total_conversions
    FROM
      `{{paths_to_conversion_table}}` AS Paths,
      UNNEST(Paths.sessions) AS S
    WHERE S.pathRecency = 1
    GROUP BY path
  ),
  NonConversionPaths AS (
    SELECT
      S.path,
      COUNT(*) AS total_non_conversions
    FROM
      `{{paths_to_non_conversion_table}}` AS Paths,
      UNNEST(Paths.sessions) AS S
    WHERE S.pathRecency = 1
    GROUP BY path
  )
SELECT
  IFNULL(Rp.path, Ncp.path) AS path,
  IFNULL(Rp.total_conversions, 0) AS total_conversions,
  IFNULL(Ncp.total_non_conversions, 0) AS total_non_conversions
FROM ConversionPaths AS Rp
FULL JOIN NonConversionPaths AS Ncp
  USING(path)
ORDER BY total_conversions DESC
