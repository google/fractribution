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

-- SQL conversion logic fragment. See extract_conversions.sql for how the fragment is included.
--
-- Use the field names described in the BigQuery export schema for Google Analytics here:
-- https://support.google.com/analytics/answer/3437719?hl=en
-- However, when referencing a repeated field, use the relevant aliases below:
--   Field: Alias
--   hits.customVariables: hitsCustomVariables
--   hits.customDimensions: hitsCustomDimensions
--   hits.customMetrics: hitsCustomMetrics
--   hits.products: hitsProducts
--   hits.promotions: hitsPromotions
--   hits.experiments: hitsExperiments
--   hits.publisher_infos: hitsPublisherInfos
--   customDimensions: customDimensions
--
-- Example 1:
totals.totalTransactionRevenue > 0
--
-- Example 2: Using hits and hits.customDimensions.
-- hits.eventInfo.eventCategory = 'customer_registration'
-- AND REGEXP_CONTAINS(hits.eventInfo.eventAction, r'complete|success')
-- AND hits.page.hostname = 'signup.your-site.com'
-- AND hitsCustomDimensions.index = 2
-- AND hitsCustomDimensions.value = 'specific_tag'
