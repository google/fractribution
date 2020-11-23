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
--
-- Note that the column subqueries are broken down into batches of 100, otherwise the BigQuery
-- planner fails because there are too many subqueries.
{% for batch in range(1 + channels|length // 100) %}
CREATE OR REPLACE TEMP TABLE ChannelConversionsTable{{batch}} AS (
  {% for channel in channels[batch * 100: (batch+1) * 100] %}
  SELECT
    '{{channel}}' AS channel,
    SUM(conversions * {{channel}}) AS conversions
  FROM `{{ path_summary_table }}`
    {% if not loop.last %}
  UNION ALL
    {% endif %}
  {% endfor %}
);
{% endfor %}

CREATE OR REPLACE TEMP TABLE ChannelConversionsTable AS (
  {% for batch in range(1 + channels|length // 100) %}
  SELECT * FROM ChannelConversionsTable{{batch}}
    {% if not loop.last %}
  UNION ALL
    {% endif %}
  {% endfor %}
);

{% for batch in range(1 + channels|length // 100) %}
CREATE OR REPLACE TEMP TABLE ChannelRevenueTable{{batch}} AS (
  {% for channel in channels[batch * 100: (batch+1) * 100] %}
  SELECT
    '{{channel}}' AS channel,
    SUM(revenue * {{channel}}) AS revenue
  FROM `{{ path_summary_table }}`
    {% if not loop.last %}
  UNION ALL
    {% endif %}
  {% endfor %}
);
{% endfor %}

CREATE OR REPLACE TEMP TABLE ChannelRevenueTable AS (
  {% for batch in range(1 + channels|length // 100) %}
  SELECT * FROM ChannelRevenueTable{{batch}}
    {% if not loop.last %}
  UNION ALL
    {% endif %}
  {% endfor %}
);

CREATE OR REPLACE TEMP TABLE ChannelSpendTable AS (
  {% include 'extract_channel_spend_data.sql' %}
);

CREATE OR REPLACE TABLE `{{ report_table }}` AS (
  SELECT
    '{{conversion_window_start_date}}' AS conversionWindowStartDate,
    '{{conversion_window_end_date}}' AS conversionWindowEndDate,
    ChannelConversionsTable.channel,
    ChannelConversionsTable.conversions,
    ChannelRevenueTable.revenue,
    ChannelSpendTable.spend,
    SAFE_DIVIDE(ChannelRevenueTable.revenue, ChannelSpendTable.spend) AS roas
  FROM ChannelConversionsTable
  LEFT JOIN ChannelRevenueTable USING (channel)
  LEFT JOIN ChannelSpendTable USING (channel)
);
