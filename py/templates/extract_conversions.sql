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

# Extracts customer conversions.
-- Args:
--  ga_sessions_table: Google Analytics BigQuery table.
--  conversion_window_start_date: Ignore conversions before this %Y-%m-%d date string.
--  conversion_window_end_date: Ignore conversions after this %Y-%m-%d date string.
--  conversion_definition_sql: Custom SQL that defines a customer conversion.
--  hostnames: Comma separated list of hostnames to restrict to.
--  fullvisitorid_userid_map_table: BigQuery table with mapping from fullVisitorId to userId.
--
-- By default, this script extracts conversions from the Google Analytics BigQuery table. If your
-- conversions are stored in a separate table, replace this script with SQL that SELECTs the data
-- with the following schema:
--    customerId: STRING NOT NULL
--      For multi-device support, set the customerId to the userId when possible. If your conversion
--      table has fullVisitorId, join it with the fullvisitorid_userid_map_table to lookup
--      corresponding userIds as below. For the final customerId, prepend it with 'u' if it is
--      a userId, and 'f' otherwise.
--    conversionTimestamp: TIMESTAMP NOT NULL
--    revenue: FLOAT64
--      Use NULL if revenue is unknown. NULL values are ignored in the final ROAS calculations.
--      However, the channels leading to this conversion will still get attribution credit.
WITH
  ConversionsByFullVisitorId AS (
    SELECT
      fullVisitorId,
      TIMESTAMP_SECONDS(
        MIN(SAFE_CAST(visitStartTime + (hits.time / 1e3) AS INT64))) AS conversionTimestamp,
      totals.totalTransactionRevenue / 1e6 AS revenue
    FROM
      `{{ga_sessions_table}}` AS Sessions,
      UNNEST(hits) AS hits
      {% if 'customDimensions.' in conversion_definition_sql %}
      -- Using LEFT JOIN because if the UNNEST is empty, a CROSS JOIN will be empty too, and we may
      -- want to inspect a separate UNNEST below.
      LEFT JOIN UNNEST(customDimensions) AS customDimensions
      {% endif %}
      {% if 'hitsCustomDimensions.' in conversion_definition_sql %}
      LEFT JOIN UNNEST(hits.customDimensions) AS hitsCustomDimensions
      {% endif %}
      {% if 'hitsCustomVariables.' in conversion_definition_sql %},
      LEFT JOIN UNNEST(hits.customVariables) AS hitsCustomVariables
      {% endif %}
      {% if 'hitsCustomMetrics.' in conversion_definition_sql %},
      LEFT JOIN UNNEST(hits.customMetrics) AS hitsCustomMetrics
      {% endif %}
      {% if 'hitsProducts.' in conversion_definition_sql %},
      LEFT JOIN UNNEST(hits.products) AS hitsProducts
      {% endif %}
      {% if 'hitsPromotions.' in conversion_definition_sql %},
      LEFT JOIN UNNEST(hits.promotions) AS hitsPromotions
      {% endif %}
      {% if 'hitsExperiments.' in conversion_definition_sql %},
      LEFT JOIN UNNEST(hits.experiments) AS hitsExperiments
      {% endif %}
      {% if 'hitsPublisherInfos.' in conversion_definition_sql %},
      LEFT JOIN UNNEST(hits.publisher_infos) AS hitsPublisherInfos
      {% endif %}
    WHERE
      _TABLE_SUFFIX BETWEEN
        FORMAT_TIMESTAMP(
          '%Y%m%d', TIMESTAMP_SUB(TIMESTAMP('{{conversion_window_start_date}}'), INTERVAL 1 DAY))
        AND FORMAT_TIMESTAMP(
          "%Y%m%d", TIMESTAMP_ADD(TIMESTAMP('{{conversion_window_end_date}}'), INTERVAL 1 DAY))
      AND visitStartTime BETWEEN
        UNIX_SECONDS('{{conversion_window_start_date}} 00:00:00 UTC')
        AND UNIX_SECONDS('{{conversion_window_end_date}} 23:59:59 UTC')
      AND (
        {% filter indent(width=8) %}{{conversion_definition_sql}}{% endfilter %}
      )
    GROUP BY
      fullVisitorId,
      visitStartTime,
      revenue
  ),
  FullVisitorIdUserIdMapTable AS (
    SELECT DISTINCT fullVisitorId, userId FROM `{{fullvisitorid_userid_map_table}}`
  )
SELECT
  CASE
    WHEN FullVisitorIdUserIdMapTable.userId IS NOT NULL
      THEN CONCAT('u', FullVisitorIdUserIdMapTable.userId)
    ELSE CONCAT('f', ConversionsByFullVisitorId.fullVisitorId)
  END AS customerId,
  conversionTimestamp,
  revenue
FROM ConversionsByFullVisitorId
LEFT JOIN FullVisitorIdUserIdMapTable USING (fullVisitorId)
-- Do not include a trailing ; as this query is included in another SQL query.

