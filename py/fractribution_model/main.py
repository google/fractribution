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


"""Main function for running fractribution over BigQuery."""

import base64
import distutils.util
import json
import logging
import os
from typing import Mapping

from jinja2 import Environment
from jinja2 import FileSystemLoader

from google.cloud import bigquery
 import fractribution
 import fractribution_report
 import tuple_transforms

_REPORT_TABLE_PARTITION_EXP_MS = 7776000000  # 90 days
env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), 'templates')))


def _get_flag_or_die(flags: Mapping[str, str], flag: str) -> str:
  """Returns the value of the given flag. Dies if the flag is not defined.

  Args:
    flags: Dictionary of (flag name, flag value)
    flag: Target flag to get the value of.
  Returns:
    Value of the given flag as a string.
  Raises:
    ValueError: User formatted error message if the flag is not defined.
  """
  value = flags.get(flag, None)
  if not value:
    raise ValueError('Missing flag: %s' % flag)
  return value


def _extract_paths(
    client: bigquery.Client, path_summary_table: str
    ) -> bigquery.job.QueryJob:
  """Extracts paths from path_summary_table.

  Args:
    client: BigQuery client.
    path_summary_table: BigQueryTable containing the following columns in order:
        1. path (string representation of the path)
        2. total_conversions
        3. total_non_conversions
  Returns:
    QueryJob with rows of (path_string, customer_id, endpoint_datetime).
  """
  return client.query(
      env.get_template(
          'select_path_summary_query.sql').render(
              path_summary_table=path_summary_table))


def _get_extract_customers_job(
    client: bigquery.Client,
    key_on_fullvisitorid: bool,
    paths_to_conversion_table: str,
    report_window_start, report_window_end,
    core_fullvisitorid_customer_id_map_table: str,
    max_map_minutes: int,
    add_unmatched_customers_from_table: str) -> bigquery.job.QueryJob:
  """Extracts customers from the input BigQuery table.

  Args:
    client: Bigquery client to run the query on.
    key_on_fullvisitorid: bool,
    paths_to_conversion_table: Name of the BigQuery table containing the paths
        to conversion.
    report_window_start: %Y-%m-%d formatted string of the report start date.
    report_window_end: %Y-%m-%d formatted string of the report end date.
    core_fullvisitorid_customer_id_map_table: Name of the BigQuery table
        containing the mapping between fullVisitorId and customer_id.
    max_map_minutes: Maximum integer time to consider a match between
        customer_id and fullVisitorId in
        core_fullvisitorid_customer_id_map_table.
    add_unmatched_customers_from_table: If non-empty, add all converted
        customers from this table. Must have customer_id and endpoint_datetime
        columns.

  Returns:
    QueryJob with rows of (path_string, customer_id, endpoint_datetime).
  """
  return client.query(env.get_template('path_customer_map_query.sql').render(
      key_on_fullvisitorid=key_on_fullvisitorid,
      paths_to_conversion_table=paths_to_conversion_table,
      report_window_start=report_window_start,
      report_window_end=report_window_end,
      core_fullvisitorid_customer_id_map_table=core_fullvisitorid_customer_id_map_table,
      max_map_minutes=max_map_minutes,
      add_unmatched_customers_from_table=add_unmatched_customers_from_table))


def main(event, unused_context):
  """Cloud Function entrypoint for running Fractribution.

  Args:
    event (dict): The dictionary with data specific to the given event.
    unused_context (google.cloud.functions.Context): The Cloud Functions event
        metadata.

  Returns:
    0 on success. Any failures are signalled by an exception being thrown.
  """
  data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
  return execute(data)


def execute(data):
  """Executes Fractribution Model.

  Stage 1. Reads in the path_summary_table and applies a transformation
           to the paths.
  Stage 2. Runs Fractribution on the paths.
  Stage 3. Joins the paths with customer data for output.

  Args:
    data (dict): The dictionary of parameters.

  Returns:
    0 on success. Any failures are signalled by an exception being thrown.
  """
  project_id = _get_flag_or_die(data, 'project_id')
  client = bigquery.Client(project=project_id)

  # Flags related to the input conversion and non-conversion summary paths.
  path_summary_table = _get_flag_or_die(data, 'path_summary_table')
  path_transform_method = data.get('path_transform_method', 'exposure')
  # Check path_transform_method is valid.
  if path_transform_method not in tuple_transforms.transform_name_to_function:
    raise ValueError('path_transform_method %s not in %s' % (
        path_transform_method,
        tuple_transforms.transform_name_to_function.keys()))
  path_transform = tuple_transforms.transform_name_to_function[
      path_transform_method]

  # Flags related to the customer-level path information.
  paths_to_conversion_table = _get_flag_or_die(
      data, 'paths_to_conversion_table')
  key_on_fullvisitorid = distutils.util.strtobool(
      data.get('key_on_fullvisitorid', 'True'))
  core_fullvisitorid_customer_id_map_table = data.get(
      'core_fullvisitorid_customer_id_map_table', '')
  if not key_on_fullvisitorid and not core_fullvisitorid_customer_id_map_table:
    raise ValueError('Flag core_fullvisitorid_customer_id_map_table is empty '
                     'when key_on_fullvisitorid=False')
  max_map_minutes = int(data.get('max_map_minutes', 20))

  # Flags related to the output report table.
  report_table_name = _get_flag_or_die(data, 'report_table')
  report_window_start = _get_flag_or_die(data, 'report_window_start')
  report_window_end = _get_flag_or_die(data, 'report_window_end')
  report_partition_exp_ms = data.get('report_table_partition_exp_ms')
  if not report_partition_exp_ms:
    report_partition_exp_ms = _REPORT_TABLE_PARTITION_EXP_MS

  default_channels = ','.join(
      [c for c in fractribution_report.DEFAULT_CHANNELS])
  channels = data.get('channels', default_channels).split(',')
  if fractribution_report.UNMATCHED_CHANNEL not in channels:
    raise ValueError('Input channel list must contain: %s' %
                     fractribution_report.UNMATCHED_CHANNEL)
  add_unmatched_customers_from_table = data.get(
      'add_unmatched_customers_from_table', None)

  report_table = bigquery.Table(
      report_table_name,
      schema=fractribution_report.get_report_schema(channels))
  report_table.time_partitioning = bigquery.TimePartitioning(
      type_=bigquery.TimePartitioningType.DAY,
      expiration_ms=_REPORT_TABLE_PARTITION_EXP_MS,
  )
  client.create_table(report_table, exists_ok=True)

  # Step 1: Extract and transform paths from the path_summary_table.
  logging.info('Extracting & transforming paths from the path_summary_table...')
  transformed_tuple_to_path, channel_to_encoding = (
      fractribution.transform_paths(_extract_paths(client, path_summary_table),
                                    path_transform))

  # Step 2: Run fractional attribution.
  logging.info('Running fractional attribution...')
  fractribution.compute_fractional_values(transformed_tuple_to_path)
  fractribution.normalize_event_names(
      transformed_tuple_to_path, path_transform_method)

  # Step 3: Join customer-level paths with path fractional attribution info and
  # upload the result to the report BigQuery table.
  # Extract customer-level path information.
  logging.info('Joining customer-level paths...')
  query_job = _get_extract_customers_job(
      client, key_on_fullvisitorid, paths_to_conversion_table,
      report_window_start, report_window_end,
      core_fullvisitorid_customer_id_map_table, max_map_minutes,
      add_unmatched_customers_from_table)
  query_job.result()
  customers_table = client.get_table(query_job.destination)
  fractribution_report.join_customers_with_attribution_paths_and_upload(
      client, customers_table, path_transform, transformed_tuple_to_path,
      channel_to_encoding, report_window_start, report_window_end, channels,
      report_table_name)
  logging.info('Done')
  return 0
