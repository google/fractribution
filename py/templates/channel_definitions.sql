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

# SQL mapping from channel definition to channel name.
-- A final catch-all 'Unmatched_Channel' must be included for unmatched channels.
-- Note: Channel names become BigQuery column names, so they must consist of letters, numbers and
--       underscores only. Also, column names must be at most 300 characters long. See
--       https://cloud.google.com/bigquery/docs/schemas#column_names for the full specification.
--
-- Default channel definitions (see the end for campaign-level definitions)
CASE
  WHEN
    LOWER(trafficSource.medium) IN ('cpc', 'ppc')
    AND REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'brand')
    THEN 'Paid_Search_Brand'
  WHEN
    LOWER(trafficSource.medium) IN ('cpc', 'ppc')
    AND REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'generic')
    THEN 'Paid_Search_Generic'
  WHEN
    LOWER(trafficSource.medium) IN ('cpc', 'ppc')
    AND NOT REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'brand|generic')
    THEN 'Paid_Search_Other'
  WHEN LOWER(trafficSource.medium) = 'organic' THEN 'Organic_Search'
  WHEN
    LOWER(trafficSource.medium) IN ('display', 'cpm', 'banner')
    AND REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'prospect')
    THEN 'Display_Prospecting'
  WHEN
    LOWER(trafficSource.medium) IN ('display', 'cpm', 'banner')
    AND REGEXP_CONTAINS(
        LOWER(trafficSource.campaign),
        r'retargeting|re-targeting|remarketing|re-marketing')
    THEN 'Display_Retargeting'
  WHEN
    LOWER(trafficSource.medium) IN ('display', 'cpm', 'banner')
    AND NOT REGEXP_CONTAINS(
        LOWER(trafficSource.campaign),
        r'prospect|retargeting|re-targeting|remarketing|re-marketing')
    THEN 'Display_Other'
  WHEN
    REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'video|youtube')
    OR REGEXP_CONTAINS(LOWER(trafficSource.source), r'video|youtube')
    THEN 'Video'
  WHEN
    LOWER(trafficSource.medium) = 'social'
    AND REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'prospect')
    THEN 'Paid_Social_Prospecting'
  WHEN
    LOWER(trafficSource.medium) = 'social'
    AND REGEXP_CONTAINS(
        LOWER(trafficSource.campaign),
        r'retargeting|re-targeting|remarketing|re-marketing')
    THEN 'Paid_Social_Retargeting'
  WHEN
    LOWER(trafficSource.medium) = 'social'
    AND NOT REGEXP_CONTAINS(
        LOWER(trafficSource.campaign),
        r'prospect|retargeting|re-targeting|remarketing|re-marketing')
    THEN 'Paid_Social_Other'
  WHEN trafficSource.source = '(direct)' THEN 'Direct'
  WHEN LOWER(trafficSource.medium) = 'referral' THEN 'Referral'
  WHEN LOWER(trafficSource.medium) = 'email' THEN 'Email'
  WHEN
    LOWER(trafficSource.medium) IN ('cpc', 'ppc', 'cpv', 'cpa', 'affiliates')
    THEN 'Other_Advertising'
  ELSE 'Unmatched_Channel'
END

-- Campaign-level channel definitions:
-- Channel name format: <medium>_<source>_<campaign>, with NULLs and any illegal BigQuery Column
--   characters replaced with '_'. If the channel name would be too long for a BigQuery column, it
--   is cropped and appended with a unique id. By default, the channel name is 'Unmatched_Channel',
--   whenever all of <medium>, <source> and <campaign> are NULL.
-- CASE
--   WHEN
--     trafficSource.medium IS NOT NULL
--     OR trafficSource.source IS NOT NULL
--     OR trafficSource.campaign IS NOT NULL
--   THEN
--     REGEXP_REPLACE(
--       IF (LENGTH(
--         ARRAY_TO_STRING([
--           'medium', trafficSource.medium,
--           'source', trafficSource.source,
--           'campaign', trafficSource.campaign], '_', '')) <= 300,
--         ARRAY_TO_STRING([
--           'medium', trafficSource.medium,
--           'source', trafficSource.source,
--           'campaign', trafficSource.campaign], '_', ''),
--         CONCAT(LEFT(ARRAY_TO_STRING([
--           'medium', trafficSource.medium,
--           'source', trafficSource.source,
--           'campaign', trafficSource.campaign], '_', ''), 279),
--           '_',
--           FARM_FINGERPRINT(ARRAY_TO_STRING([
--             'medium', trafficSource.medium,
--             'source', trafficSource.source,
--             'campaign', trafficSource.campaign], '_', ''))
--         )
--       ), '[^a-zA-Z0-9_]','_')
-- ELSE
--   'Unmatched_Channel'
-- END
