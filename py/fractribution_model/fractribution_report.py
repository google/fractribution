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


"""Helpers for uploading fractribution data into the final BigQuery report."""

import re
from typing import Any, Callable, Iterable, List, Mapping, Tuple
 import tuple_path
from google.cloud import bigquery

# Maximum batch size for row insertion.
_MAX_ROW_BATCH_SIZE = 10000
# Default channel names. Note that channel names can be overwritten, however
# the every channel list must include UNMATCHED_CHANNEL below.
UNMATCHED_CHANNEL = 'Unmatched Channel'
DEFAULT_CHANNELS = (
    'Direct',
    'Organic Search',
    'Paid Search - Brand',
    'Paid Search - Generic',
    'Paid Search - Other',
    'Display - Prospecting',
    'Display - Retargeting',
    'Display - Other',
    'Video',
    'Paid Social - Prospecting',
    'Paid Social - Retargeting',
    'Paid Social - Other',
    'Referral',
    'Email',
    'Other Advertising',
    UNMATCHED_CHANNEL
)


def _format_channel(channel: str) -> str:
  """Returns the channel name formatted as a legal BigQuery column name.

  Args:
    channel: Name of the channel as a string.

  Returns:
    Formatted version of the channel with illegal characters replaced by _.
  """
  return re.sub(r'[^0-9a-zA-Z_]+', '_', channel.strip().lower())


def get_report_schema(channels: Iterable[str] = DEFAULT_CHANNELS
                     ) -> List[bigquery.SchemaField]:
  """Returns a list of schema fields for the output report.

  Args:
    channels: Ordered list of channel names. Default: DEFAULT_CHANNELS.

  Returns:
    Ordered list of schema fields for the output report.
  """
  schema_fields = [
      bigquery.SchemaField('report_window_start', 'DATE', mode='REQUIRED'),
      bigquery.SchemaField('report_window_end', 'DATE', mode='REQUIRED'),
      bigquery.SchemaField('path', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('customer_id', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('endpoint_datetime', 'INTEGER', mode='REQUIRED')
  ]
  for channel in channels:
    schema_fields.append(bigquery.SchemaField(
        _format_channel(channel), 'FLOAT', mode='REQUIRED'))
  schema_fields.append(bigquery.SchemaField(
      'total_attribution', 'FLOAT', mode='REQUIRED'))
  return schema_fields


def construct_report_table_row(
    report_window_start: str,
    report_window_end: str,
    path_string: str,
    customer_id: str,
    endpoint_datetime: int,
    channels: Iterable[str],
    event_to_attribution: Mapping[str, float]) -> Tuple[Any, ...]:
  """Returns a report table row as a tuple of column values.

  Args:
    report_window_start: %Y-%m-%d formatted string of the report start date.
    report_window_end: %Y-%m-%d formatted string of the report end date.
    path_string: String representation of the conversion path.
    customer_id: Id of the customer as a string.
    endpoint_datetime: Timestamp offset in seconds.
    channels: List of all media channels as strings. Note that the channels must
      be in the same order as the report schema.
    event_to_attribution: Dict from event (channel) to float attribution value.

  Returns:
    Tuple of values, one for each column in the Report BigQuery schema.
  """
  row = [report_window_start, report_window_end,
         path_string, customer_id, endpoint_datetime]
  row_sum = 0
  for channel in channels:
    attribution_value = event_to_attribution.get(channel, 0.0)
    row.append(attribution_value)
    row_sum += attribution_value
  for channel in event_to_attribution:
    if channel not in channels:
      raise ValueError('Missing channel %s from channel list.' % channel)
  row.append(row_sum)
  return tuple(row)


def insert_rows(
    client: bigquery.Client,
    table: str,
    rows: List[Tuple[Any, ...]],
    max_batch_size: int = _MAX_ROW_BATCH_SIZE) -> None:
  """Inserts the given rows into the given BigQuery table.

  Args:
    client: BigQuery client
    table: String table name.
    rows: List of rows, where each row is tuple of column values.
    max_batch_size: Maximum number of rows to insert in one batch.
  Raises:
    RuntimeError: Encapsulates any error inserting the rows into the table.
  """
  for i in range(0, len(rows), max_batch_size):
    errors = client.insert_rows(
        client.get_table(table), rows[i:i + max_batch_size])
    if errors:
      raise RuntimeError(
          'Error inserting rows into table %s: %s' % (table, errors))


def get_path_and_event_to_attribution(
    path_transform: Callable[[Tuple[str, ...]], Tuple[str, ...]],
    transformed_tuple_to_path: Mapping[Tuple[str, ...], tuple_path.Path],
    channel_to_encoding: Mapping[str, str],
    path_string: str,
    customer_id: str) -> Tuple[str, Mapping[str, float]]:
  """Returns the customer's unencoded path string and event_to_attribution Dict.

  Args:
    path_transform: Function for transforming a path string into a path tuple.
    transformed_tuple_to_path: Dict from transformed tuple to tuple_path.Path.
    channel_to_encoding: Dict from channel name to encoded channel name.
    path_string: Encoded path string for the customer.
    customer_id: Id for the customer.
  Returns:
    Tuple of unencoded path_string and event_to_attribution Dict.
  """
  encoding_to_channel = {
      encoding: channel for channel, encoding in channel_to_encoding.items()
  }
  default_event_to_attribution = {UNMATCHED_CHANNEL: 1.0}
  if not path_string:
    return path_string, default_event_to_attribution
  encoded_channels = []
  encoding_error = False
  for channel in path_string.split(' > '):
    if channel not in channel_to_encoding:
      encoding_error = True
      print('Error: Could not find channel mapping for:',
            channel, flush=True)
      break
    encoded_channels.append(channel_to_encoding[channel])
  if encoding_error:
    return path_string, default_event_to_attribution
  transformed_tuple = path_transform(tuple(encoded_channels))
  path = transformed_tuple_to_path[transformed_tuple]
  if not path.event_to_attribution:
    print('Error: could not find attribution for customer',
          customer_id, path_string, flush=True)
    return path_string, default_event_to_attribution
  return path.get_path_string(encoding_to_channel), {
      encoding_to_channel[event]: attribution
      for (event, attribution) in path.event_to_attribution.items()
  }


def join_customers_with_attribution_paths(
    customers: Iterable[Tuple[str, str, int]],
    path_transform: Callable[[Tuple[str, ...]], Tuple[str, ...]],
    transformed_tuple_to_path: Mapping[Tuple[str, ...], tuple_path.Path],
    channel_to_encoding: Mapping[str, str],
    report_window_start: str,
    report_window_end: str,
    channels: Iterable[str]) -> List[Tuple[Any, ...]]:
  """Joins path-level attribution data with customer paths.

  Args:
    customers: List of (path_string, customer_id, endpoint_datetime) tuples.
    path_transform: Function for transforming a path string into a path tuple.
    transformed_tuple_to_path: Dict from transformed tuple to tuple_path.Path.
    channel_to_encoding: Dict from channel name to encoded channel name.
    report_window_start: %Y-%m-%d formatted string of the report start date.
    report_window_end: %Y-%m-%d formatted string of the report end date.
    channels: List of all media channels as strings.
  Returns:
    List of tuples, one for each customer, consisting of an ordered sequence of
    column values for the customer row in the output table.
  """
  rows = []
  for (path_string, customer_id, endpoint_datetime) in customers:
    path_string, event_to_attribution = get_path_and_event_to_attribution(
        path_transform,
        transformed_tuple_to_path,
        channel_to_encoding,
        path_string,
        customer_id)
    rows.append(construct_report_table_row(
        report_window_start, report_window_end, path_string,
        customer_id, endpoint_datetime, channels, event_to_attribution))
  return rows


def join_customers_with_attribution_paths_and_upload(
    client: bigquery.Client,
    customers_table: bigquery.table.Table,
    path_transform: Callable[[Tuple[str, ...]], Tuple[str, ...]],
    transformed_tuple_to_path: Mapping[Tuple[str, ...], tuple_path.Path],
    channel_to_encoding: Mapping[str, str],
    report_window_start: str,
    report_window_end: str,
    channels: Iterable[str],
    report_table: str) -> None:
  """Joins path-level attribution data with customer paths.

  Args:
    client: BigQuery client
    customers_table: Table of path_string, customer_id, endpoint_datetime.
    path_transform: Function for transforming a path string into a path tuple.
    transformed_tuple_to_path: Dict from transformed tuple to tuple_path.Path.
    channel_to_encoding: Dict from channel name to encoded channel name.
    report_window_start: %Y-%m-%d formatted string of the report start date.
    report_window_end: %Y-%m-%d formatted string of the report end date.
    channels: List of all media channels as strings. Note that the channels must
        be in the same order as the report schema.
    report_table: Table to write the output report.
  """
  rows = []
  for (path_string, customer_id, endpoint_datetime) in client.list_rows(
      customers_table, page_size=_MAX_ROW_BATCH_SIZE):
    path_string, event_to_attribution = get_path_and_event_to_attribution(
        path_transform,
        transformed_tuple_to_path,
        channel_to_encoding,
        path_string,
        customer_id)
    rows.append(construct_report_table_row(
        report_window_start, report_window_end, path_string,
        customer_id, endpoint_datetime, channels, event_to_attribution))
    if len(rows) >= _MAX_ROW_BATCH_SIZE:
      insert_rows(client, report_table, rows)
      # Explicitly delete the rows and free up memory. Reassignment to the empty
      # list alone can cause the Cloud Function to exceed the maximum 2GB memory
      # limit.
      del rows[:]
      rows = []
  if rows:
    insert_rows(client, report_table, rows)
    # Explicitly delete the rows and free up memory. Reassignment to the empty
    # list alone can cause the Cloud Function to exceed the maximum 2GB memory
    # limit.
    del rows[:]
