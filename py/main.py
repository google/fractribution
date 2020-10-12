# coding=utf-8
# Copyright 2021 Google LLC..
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


"""Loads the data into BigQuery needed to run Fractribution."""

import base64
import datetime
import json
import os
import re
from typing import Any, Dict, List, Mapping
import jinja2
from google.cloud import bigquery
import fractribution

_FULLVISITORID_USERID_MAP_TABLE = 'fullvisitorid_userid_map_table'
_GA_SESSIONS_TABLE = 'ga_sessions_table'
_PATHS_TO_CONVERSION_TABLE = 'paths_to_conversion_table'
_PATHS_TO_NON_CONVERSION_TABLE = 'paths_to_non_conversion_table'
_PATH_SUMMARY_TABLE = 'path_summary_table'
_CHANNEL_COUNTS_TABLE = 'channel_counts_table'
_REPORT_TABLE = 'report_table'
_CONVERSIONS_BY_CUSTOMER_ID_TABLE = 'ConversionsByCustomerId'
_SESSIONS_BY_CUSTOMER_ID_TABLE = 'SessionsByCustomerId'
_PATH_TRANSFORMS_MAP = {
    'unique': 'Unique',
    'exposure': 'Exposure',
    'first': 'First',
    'frequency': 'Frequency'
}
_REPORT_TABLE_PARTITION_EXP_MS = 7776000000  # 90 days

EXTRACT_CHANNEL_NAME_PATTERN = re.compile(
    r'(?:WHEN\s+.*?\s+?THEN|ELSE)\s+?\'(.*?)\'', re.DOTALL|re.IGNORECASE)
VALID_CHANNEL_NAME_PATTERN = re.compile(r'^[a-zA-Z_]\w+$', re.ASCII)

env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(
        os.path.join(os.path.dirname(__file__), 'templates')))


def _strip_sql(sql: str) -> str:
  """Returns a copy of sql with empty lines and -- comments stripped.

  Args:
    sql: A SQL string.
  Returns:
    A copy of sql with empty lines and -- comments removed.
  """
  lines = []
  for line in sql.split('\n'):
    line = re.sub(r'\s*--.*', '', line)
    if line.strip():
      lines.append(line)
  return '\n'.join(lines)


def _extract_channel_names(channel_definitions_sql: str) -> List[str]:
  """Returns the list of channel names from the given sql fragment.

  Args:
    channel_definitions_sql: SQL string from channel_definitions.sql
  Returns:
    List of channel name strings in the channel_definitions_sql string.
  """
  return EXTRACT_CHANNEL_NAME_PATTERN.findall(channel_definitions_sql)


def _is_valid_column_name(column_name: str) -> bool:
  """Returns True if the column_name is a valid BigQuery column name."""
  return VALID_CHANNEL_NAME_PATTERN.match(column_name) is not None


def _get_flag_or_die(flags: Mapping[str, Any], flag: str) -> Any:
  """Returns value of given flag. Dies with user-formatted message not defined.

  Args:
    flags: Dictionary of all flag names to flag values.
    flag: Name of the flag to get the value of.
  Returns:
    Value of the given flag.
  Raises:
    ValueError: User formatted message on error.
  """
  value = flags.get(flag, None)
  if not value:
    raise ValueError('Missing flag: %s' % flag)
  return value


def _get_table_name(
    project: str, dataset: str, table: str, date_suffix: str) -> str:
  """Returns the name of the table in dotted format."""
  return '{}.{}.{}_{}'.format(project, dataset, table, date_suffix)


def _update_table_name_flags(flags: Dict[str, Any]) -> None:
  """Updates flags to contain all the required table names.

  Side-effect: Adds standard table names to the flags.

  Args:
    flags: Dictionary of all flag names to flag values.
  Raises:
    ValueError: User formatted message on error.
  """
  ga_sessions_table = _get_flag_or_die(flags, _GA_SESSIONS_TABLE)
  if ga_sessions_table[-2:] != '_*':
    raise ValueError('ga_sessions_table flag must end in _*')
  project_id = _get_flag_or_die(flags, 'project_id')
  dataset = _get_flag_or_die(flags, 'dataset')
  date_suffix = datetime.datetime.strptime(
      _get_flag_or_die(flags, 'conversion_window_end_date'),
      '%Y-%m-%d').strftime('%Y%m%d')
  flags[_FULLVISITORID_USERID_MAP_TABLE] = _get_table_name(
      project_id, dataset, _FULLVISITORID_USERID_MAP_TABLE, date_suffix)
  flags[_PATHS_TO_CONVERSION_TABLE] = _get_table_name(
      project_id, dataset, _PATHS_TO_CONVERSION_TABLE, date_suffix)
  flags[_PATHS_TO_NON_CONVERSION_TABLE] = _get_table_name(
      project_id, dataset, _PATHS_TO_NON_CONVERSION_TABLE, date_suffix)
  flags[_PATH_SUMMARY_TABLE] = _get_table_name(
      project_id, dataset, _PATH_SUMMARY_TABLE, date_suffix)
  flags[_CHANNEL_COUNTS_TABLE] = _get_table_name(
      project_id, dataset, _CHANNEL_COUNTS_TABLE, date_suffix)
  flags[_REPORT_TABLE] = _get_table_name(
      project_id, dataset, _REPORT_TABLE, date_suffix)
  # Add internal tablenames for conversion and sessions.
  flags['conversions_by_customer_id_table'] = _CONVERSIONS_BY_CUSTOMER_ID_TABLE
  flags['sessions_by_customer_id_table'] = _SESSIONS_BY_CUSTOMER_ID_TABLE


def _update_conversion_window_date_flags(flags: Dict[str, Any]) -> None:
  """Updates the conversion window date flags in flags.

  Side-effect: Adds conversion_window_start_date and conversion_window_end_date
    to the flags.

  Args:
    flags: Dictionary of all flag names to flag values.
  Raises:
    ValueError: User formatted message on error.
  """
  conversion_window_length = int(flags['conversion_window_length'])
  if conversion_window_length < 0:
    raise ValueError('conversion_window_length must be non-negative.')
  flags['conversion_window_length'] = conversion_window_length
  if ('conversion_window_end_date' in flags) == (
      'conversion_window_end_today_offset_days' in flags):
    raise ValueError('Specify either conversion_window_end_date or '
                     'conversion_window_end_today_offset_days')
  end_date = None
  if 'conversion_window_end_today_offset_days' in flags:
    offset_days = int(flags['conversion_window_end_today_offset_days'])
    if offset_days < 0:
      raise ValueError('conversion_window_end_today_offset_days is negative.')
    end_date = (datetime.date.today() - datetime.timedelta(days=offset_days))
    flags['conversion_window_end_date'] = end_date.isoformat()
  else:
    end_date = datetime.datetime.strptime(
        _get_flag_or_die(flags, 'conversion_window_end_date'),
        '%Y-%m-%d').date()
    if end_date > datetime.date.today():
      raise ValueError('conversion_window_end_date is in the future.')
  start_date = end_date - datetime.timedelta(days=conversion_window_length)
  flags['conversion_window_start_date'] = start_date.isoformat()


def _update_path_lookback_flags(flags: Dict[str, Any]) -> None:
  """Updates the path lookback flags in flags.

  Side-effect: Adds a default no-limit path_lookback_steps to the flags.

  Args:
    flags: Dictionary of all flag names to flag values.
  Raises:
    ValueError: User formatted message on error.
  """
  path_lookback_days = int(_get_flag_or_die(flags, 'path_lookback_days'))
  if path_lookback_days < 1:
    raise ValueError('path_lookback_days=%d must be a positive integer.'
                     % path_lookback_days)
  flags['path_lookback_days'] = path_lookback_days
  if 'path_lookback_steps' in flags:
    path_lookback_steps = int(flags['path_lookback_steps'])
    if path_lookback_steps < 1:
      raise ValueError('path_lookback_steps=%d must be a positive integer.'
                       % path_lookback_steps)
    flags['path_lookback_steps'] = path_lookback_steps
  else:
    flags['path_lookback_steps'] = 0


def _update_channel_flags(flags: Dict[str, Any]) -> None:
  """Updates the channel definitions from the channel flags.

  Side-effect: Adds channels, a list of channel names, to the flags.

  Args:
    flags: Dictionary of all flag names to flag values.
  Raises:
    ValueError: User formatted message on error.
  """
  channel_definitions = env.get_template('channel_definitions.sql').render()
  flags['channels'] = _extract_channel_names(channel_definitions)
  if fractribution.UNMATCHED_CHANNEL not in flags['channels']:
    raise ValueError('Channel definitions must include %s.' %
                     fractribution.UNMATCHED_CHANNEL)
  for channel in flags['channels']:
    if not _is_valid_column_name(channel):
      raise ValueError(
          'Channel name %s is not a valid BigQuery column name.' % channel)


def _update_fullvisitorid_userid_map_flags(flags: Dict[str, Any]) -> None:
  """Updates the flags related to the fullvisitorid_userid_map in flags.

  Side-effect: Adds a default update_fullvisitorid_userid_map=True to the flags.

  Args:
    flags: Dictionary of all flag names to flag values.
  Raises:
    ValueError: User formatted message on error.
  """
  # Set the default behavior to update the id map.
  if 'update_fullvisitorid_userid_map' not in flags:
    flags['update_fullvisitorid_userid_map'] = True
  if 'userid_ga_custom_dimension_index' in flags:
    flags['userid_ga_custom_dimension_index'] = int(
        flags['userid_ga_custom_dimension_index'])
    if flags['userid_ga_custom_dimension_index'] < 1:
      raise ValueError(
          'userid_ga_custom_dimension_index=%s must be a positive integer' %
          flags['userid_ga_custom_dimension_index'])
  else:
    flags['userid_ga_custom_dimension_index'] = 0
  if 'userid_ga_hits_custom_dimension_index' in flags:
    flags['userid_ga_hits_custom_dimension_index'] = int(
        flags['userid_ga_hits_custom_dimension_index'])
    if flags['userid_ga_hits_custom_dimension_index'] < 1:
      raise ValueError(
          'userid_ga_hits_custom_dimension_index=%s must be a positive integer'
          % flags['userid_ga_hits_custom_dimension_index'])
  else:
    flags['userid_ga_hits_custom_dimension_index'] = 0


def update_input_flags(flags: Dict[str, Any]) -> None:
  """Main function for augmenting and running precondition checks on all flags.

  Side-effect: Reforms the hostnames flag, if present, for SQL inclusion.

  Args:
    flags: Dictionary of all flag names to flag values.
  Raises:
    ValueError: User formatted message on error.
  """
  _get_flag_or_die(flags, 'project_id')
  _get_flag_or_die(flags, 'dataset')
  _update_conversion_window_date_flags(flags)
  _update_table_name_flags(flags)
  _update_path_lookback_flags(flags)
  _update_channel_flags(flags)
  _update_fullvisitorid_userid_map_flags(flags)
  # Process the hostname restrictions.
  if 'hostnames' in flags:
    flags['hostnames'] = ', '.join(
        ["'%s'" % hostname for hostname in flags['hostnames'].split(',')])
  # Check the path_transform.
  path_transform = _get_flag_or_die(flags, 'path_transform')
  if path_transform not in _PATH_TRANSFORMS_MAP.keys():
    raise ValueError(
        'Unknown path_transform. Use one of: ', _PATH_TRANSFORMS_MAP.keys())
  flags['path_transforms'] = _PATH_TRANSFORMS_MAP[path_transform]


def extract_fractribution_input_data(
    client: bigquery.client.Client, flags: Mapping[str, Any]) -> None:
  """Extracts the input data for fractribution into BigQuery.

  Args:
    client: BigQuery client.
    flags: Dictionary of all flag names to flag values.
  """
  extract_data_sql = _strip_sql(
      env.get_template('extract_data.sql').render(flags))
  # Issue the query, and call result() to wait for it to finish. No results
  # are returned as all output is stored on BigQuery.
  client.query(extract_data_sql).result()


def run_fractribution(
    client: bigquery.client.Client, flags: Mapping[str, Any]) -> None:
  """Runs fractribution on the extract_fractribution_input_data BigQuery tables.

  Args:
    client: BigQuery client.
    flags: Dictionary of all flag names to flag values.
  """

  # Step 1: Extract the paths from the path_summary_table.
  frac = fractribution.Fractribution(client.query(
      env.get_template('select_path_summary_query.sql').render(
          path_summary_table=flags['path_summary_table'])))
  # Step 2: Run Fractribution
  frac.run_fractribution()
  frac.normalize_channel_to_attribution_names()
  # Step 3: Create the path_summary_table and upload the results.
  create_path_summary_table_sql = env.get_template(
      'create_path_summary_results_table.sql').render(flags)
  client.query(create_path_summary_table_sql).result()
  frac.upload_path_summary(client, flags['path_summary_table'])


def generate_report(
    client: bigquery.client.Client, flags: Mapping[str, Any]) -> None:
  """Generates the final BigQuery Table with channel-level attribution and ROAS.

  Args:
    client: BigQuery client.
    flags: Dictionary of all flag names to flag values.
  """
  client.query(env.get_template('generate_report.sql').render(flags)).result()


def run(flags) -> int:
  """Main entry point to run Fractribution with the given flags.

  Args:
    flags: Dictionary of parameter name to value
  Returns:
    0 on success and non-zero otherwise
  """
  update_input_flags(flags)
  client = bigquery.Client(flags['project_id'])
  client.create_dataset(
      bigquery.Dataset('{}.{}'.format(flags['project_id'], flags['dataset'])),
      exists_ok=True)
  extract_fractribution_input_data(client, flags)
  run_fractribution(client, flags)
  generate_report(client, flags)
  return 0


def main(event, unused_context=None) -> int:
  """Entry point for Cloud Function."""
  flags = json.loads(base64.b64decode(event['data']).decode('utf-8'))
  return run(flags)
