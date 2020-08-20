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

# Construct the fractribution report table, with channel level conversions, revenue spend and ROAS.
--
-- Aggregates fractional conversion and revenue data by channel, joins it with channel-level spend
-- if supplied, and computes channel-level return on ad spend (ROAS).
--
-- Note that revenue data is not required. If revenue is NULL for a customer conversion, the
-- conversion is ignored in the ROAS calculation. However, the channels on the path to conversion
-- will still receive fractional attribution for the conversion.
--
-- Although the column name is revenue, alternative values can be substituted, like predicted
-- customer-lifetime value.
CREATE OR REPLACE TABLE {{ report_table }} AS (
  WITH
    ConversionsTable AS (
      {% for channel in channels %}
      SELECT
        '{{channel}}' AS channel,
        SUM(conversions * {{channel}}) AS conversions
      FROM {{ path_summary_table }}
      {% if not loop.last %}
      UNION ALL
      {% endif %}
      {% endfor %}
   ), RevenueTable AS (
     {% for channel in channels %}
     SELECT
       '{{channel}}' AS channel,
       SUM(revenue * {{channel}}) AS revenue
     FROM {{ path_summary_table }}
     {% if not loop.last %}
     UNION ALL
     {% endif %}
     {% endfor %}
  ), ChannelSpendTable AS (
    {% include 'extract_channel_spend_data.sql' %}
  )
  SELECT
    '{{conversion_window_start_date}}' AS conversionWindowStartDate,
    '{{conversion_window_end_date}}' AS conversionWindowEndDate,
    channel,
    conversions,
    revenue,
    spend,
    revenue / spend AS roas
  FROM ConversionsTable
  INNER JOIN RevenueTable USING (channel)
  LEFT JOIN ChannelSpendTable USING (channel)
)
