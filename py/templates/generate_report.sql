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

CREATE OR REPLACE TEMP TABLE ChannelSpendTable AS (
  {% include 'extract_channel_spend_data.sql' %}
);

CREATE OR REPLACE TABLE `{{report_table}}` AS (
  SELECT
    ConversionRevenueTable.conversionWindowStartDate,
    ConversionRevenueTable.conversionWindowEndDate,
    ConversionRevenueTable.channel,
    ConversionRevenueTable.conversions,
    ConversionRevenueTable.revenue,
    ChannelSpendTable.spend,
    SAFE_DIVIDE(ConversionRevenueTable.revenue, ChannelSpendTable.spend) AS roas
  FROM `{{report_table}}` AS ConversionRevenueTable
  LEFT JOIN ChannelSpendTable USING (channel)
  ORDER BY conversions DESC
);
