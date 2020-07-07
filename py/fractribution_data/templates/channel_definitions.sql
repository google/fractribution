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

# SQL mapping from channel definition to channel name.
CASE
  WHEN
    LOWER(trafficSource.medium) IN ('cpc', 'ppc')
    AND REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'brand')
    THEN 'Paid Search - Brand'
  WHEN
    LOWER(trafficSource.medium) IN ('cpc', 'ppc')
    AND REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'generic')
    THEN 'Paid Search - Generic'
  WHEN
    LOWER(trafficSource.medium) IN ('cpc', 'ppc')
    AND NOT REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'brand|generic')
    THEN 'Paid Search - Other'
  WHEN LOWER(trafficSource.medium) = 'organic' THEN 'Organic Search'
  WHEN
    LOWER(trafficSource.medium) IN ('display', 'cpm', 'banner')
    AND REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'prospect')
    THEN 'Display - Prospecting'
  WHEN
    LOWER(trafficSource.medium) IN ('display', 'cpm', 'banner')
    AND REGEXP_CONTAINS(
        LOWER(trafficSource.campaign),
        r'retargeting|re-targeting|remarketing|re-marketing')
    THEN 'Display - Retargeting'
  WHEN
    LOWER(trafficSource.medium) IN ('display', 'cpm', 'banner')
    AND NOT REGEXP_CONTAINS(
        LOWER(trafficSource.campaign),
        r'prospect|retargeting|re-targeting|remarketing|re-marketing')
    THEN 'Display - Other'
  WHEN
    REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'video|youtube')
    OR REGEXP_CONTAINS(LOWER(trafficSource.source), r'video|youtube')
    THEN 'Video'
  WHEN
    LOWER(trafficSource.medium) = 'social'
    AND REGEXP_CONTAINS(LOWER(trafficSource.campaign), r'prospect')
    THEN 'Paid Social - Prospecting'
  WHEN
    LOWER(trafficSource.medium) = 'social'
    AND REGEXP_CONTAINS(
        LOWER(trafficSource.campaign),
        r'retargeting|re-targeting|remarketing|re-marketing')
    THEN 'Paid Social - Retargeting'
  WHEN
    LOWER(trafficSource.medium) = 'social'
    AND NOT REGEXP_CONTAINS(
        LOWER(trafficSource.campaign),
        r'prospect|retargeting|re-targeting|remarketing|re-marketing')
    THEN 'Paid Social - Other'
  WHEN trafficSource.source = '(direct)' THEN 'Direct'
  WHEN LOWER(trafficSource.medium) = 'referral' THEN 'Referral'
  WHEN LOWER(trafficSource.medium) = 'email' THEN 'Email'
  WHEN
    LOWER(trafficSource.medium) IN ('cpc', 'ppc', 'cpv', 'cpa', 'affiliates')
    THEN 'Other Advertising'
  ELSE 'Unmatched Channel'
END AS channel
