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

# SQL script for extracting the paths to conversion and non-conversion used in Fractribution.
-- Args:
--  fullvisitorid_userid_map_table
--  update_fullvisitorid_userid_map
--  extract_conversions_sql: Custom SQL for extracting all conversions.
--  paths_to_conversion_table
--  paths_to_non_conversion_table
--  path_summary_table
--  channel_counts_table

# Create the fullvisitorid_userid_map_table if it does not exist.
CREATE TABLE IF NOT EXISTS `{{fullvisitorid_userid_map_table}}` (
  fullVisitorId STRING NOT NULL,
  userId STRING NOT NULL,
  mapStartTimestamp TIMESTAMP NOT NULL,
  tableSuffixWhenAdded STRING NOT NULL
);
{% if update_fullvisitorid_userid_map %}
INSERT `{{fullvisitorid_userid_map_table}}`
  (fullVisitorId, userId, mapStartTimestamp, tableSuffixWhenAdded)
{% include 'extract_fullvisitorid_userid_map.sql' %};
{% endif %}

CREATE TEMP TABLE ConversionsByCustomerId AS (
{% filter indent(width=2) %}
{{extract_conversions_sql}}
{% endfilter %}
-- Including blank line to force a newline, in case extract_conversions.sql ends with a comment.

);

CREATE TEMP TABLE SessionsByCustomerId AS (
{% filter indent(width=2) %}
{% include 'extract_ga_sessions.sql' %}
{% endfilter %}
-- Including blank line to force a newline, in case extract_ga_sessions.sql ends with a comment.

);

{% include 'path_transforms.sql' %}

CREATE OR REPLACE TABLE `{{paths_to_conversion_table}}` AS (
{% filter indent(width=2) %}
{% include 'paths_to_conversion.sql' %}
{% endfilter %}
);

CREATE OR REPLACE TABLE `{{paths_to_non_conversion_table}}` AS (
{% filter indent(width=2) %}
{% include 'paths_to_non_conversion.sql' %}
{% endfilter %}
);

CREATE OR REPLACE TABLE `{{path_summary_table}}` AS (
{% filter indent(width=2) %}
{% include 'path_summary.sql' %}
{% endfilter %}
);

CREATE OR REPLACE TABLE `{{channel_counts_table}}` AS (
{% filter indent(width=2) %}
{% include 'channel_counts.sql' %}
{% endfilter %}
);
