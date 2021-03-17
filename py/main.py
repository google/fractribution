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

# python3 main.py
#--project_id=dabraham-gawindowingpipeline --dataset=fractribution --region=us-central1 --ga_sessions_table=bigquery-public-data.google_analytics_sample.ga_sessions_* --conversion_window_end_date=2017-08-01 --conversion_window_length=30 --path_lookback_days=30 --path_transform=exposure


"""Loads the data into BigQuery needed to run Fractribution."""

import base64
import datetime
import json
import os
import re
from typing import Any, Dict, List, Mapping
from absl import app
from absl import flags
from google.cloud import bigquery
import jinja2
import fractribution

FLAGS = flags.FLAGS

# GCP flags.
flags.DEFINE_string('project_id', None, 'Google Cloud project to run inside.')
flags.DEFINE_string('dataset', None, 'BigQuery dataset to write the output.')
flags.DEFINE_string(
    'region', None, 'Region to create the dataset if it does not exist (see '
    'https://cloud.google.com/bigquery/docs/locations).')

# Google Analytics flags.
flags.DEFINE_string(
    'ga_sessions_table', None, 'Name of the GA360 BigQuery table in the format '
    '`<PROJECT>.<DATASET>.<TABLE>_*`.')
flags.DEFINE_string(
    'hostnames', None,
    'Comma separated list of hostnames. Restrict user sessions '
    'to this set of hostnames (Default: no restriction).')

# Conversion window flags.
flags.DEFINE_integer('conversion_window_length', None,
                     'Number of days in the conversion window.')
flags.DEFINE_string('conversion_window_end_date', None,
                    'Ignore conversions after this YYYY-MM-DD UTC date.')
flags.DEFINE_integer(
    'conversion_window_end_today_offset_days', None,
    'Set the conversion window end date to this many days '
    'before today. This is an alternative to '
    'conversion_window_end_date used in regular scheduled '
    'runs of fractribution.')

# Path flags
flags.DEFINE_integer(
    'path_lookback_days', None,
    'Number of days in a user\'s path to (non)conversion. '
    'Recommended values: 30, 14, or 7.')
flags.DEFINE_integer(
    'path_lookback_steps', None,
    'Optional limit on the number of steps/marketing-channels '
    'in a user\'s path to (non)conversion to the most recent '
    'path_lookback_steps. (Default: no restriction).')
flags.DEFINE_string(
    'path_transform', 'exposure',
    'Name of the path transform function for changing user '
    'paths to improve matching and performance on sparse data. '
    'Options: unique, exposure, first, frequency. See the '
    'README for more details.')

# UserId mapping
flags.DEFINE_boolean(
    'update_fullvisitorid_userid_map', True,
    'True to update the internal map from fullVisitorId to '
    'userId, and False otherwise. (Default: True).')
flags.DEFINE_integer(
    'userid_ga_custom_dimension_index', None,
    'Index of the GA custom dimension storing the non-Google '
    'userId. If set, a map is created between Google '
    'fullVisitorIds and userIds. (Default: no index).')
flags.DEFINE_integer(
    'userid_ga_hits_custom_dimension_index', None,
    'Index of the GA hit-level custom dimension storing the '
    'non-Google userId. If set, a map is created between '
    'Google fullVisitorIds and userIds. (Default: no index).')

_FULLVISITORID_USERID_MAP_TABLE = 'fullvisitorid_userid_map_table'
_PATHS_TO_CONVERSION_TABLE = 'paths_to_conversion_table'
_PATHS_TO_NON_CONVERSION_TABLE = 'paths_to_non_conversion_table'
_PATH_SUMMARY_TABLE = 'path_summary_table'
_CHANNEL_COUNTS_TABLE = 'channel_counts_table'
_REPORT_TABLE = 'report_table'
_OUTPUT_TABLES = [
    _FULLVISITORID_USERID_MAP_TABLE, _PATHS_TO_CONVERSION_TABLE,
    _PATHS_TO_NON_CONVERSION_TABLE, _PATH_SUMMARY_TABLE, _CHANNEL_COUNTS_TABLE,
    _REPORT_TABLE
]

_PATH_TRANSFORMS_MAP = {
    'unique': 'Unique',
    'exposure': 'Exposure',
    'first': 'First',
    'frequency': 'Frequency'
}

jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(
        os.path.join(os.path.dirname(__file__), 'templates')),
    keep_trailing_newline=True,
    lstrip_blocks=True,
    trim_blocks=True)


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


def _get_param_or_die(input_params: Mapping[str, Any], param: str) -> Any:
  """Returns value of param. Dies with user-formatted message if not defined.

  Args:
    input_params: Mapping from input parameter names to values.
    param: Name of the param to get the value of.
  Returns:
    Value of the given param.
  Raises:
    ValueError: User formatted message on error.
  """
  value = input_params.get(param, None)
  if not value:
    raise ValueError('Missing parameter: %s' % param)
  return value


def parse_int_param(input_params: Mapping[str, Any],
                    param: str,
                    lower_bound: int = None,
                    upper_bound: int = None) -> Any:
  """Returns int value of param.

  Dies with user-formatted message on error.

  Args:
    input_params: Mapping from input parameter names to values.
    param: Name of the param to get the value of.
    lower_bound: If not None, the value must be at least lower_bound.
    upper_bound: If not None, the value must be at most upper_bound.

  Returns:
    Integer value of the given param.
  Raises:
    ValueError: User formatted message on error.
  """
  value = _get_param_or_die(input_params, param)
  try:
    int_value = int(value)
  except ValueError:
    raise ValueError('Parameter %s must be an int' % param)
  if lower_bound and int_value < lower_bound:
    raise ValueError('Parameter %s must be at least %i' % (param, lower_bound))
  if upper_bound and int_value > upper_bound:
    raise ValueError('Parameter %s must be at most %i' % (param, upper_bound))
  return int_value


def _get_table_name(
    project: str, dataset: str, table: str, date_suffix: str) -> str:
  """Returns the name of the table in BigQuery dotted format."""
  return '{}.{}.{}_{}'.format(project, dataset, table, date_suffix)


def _get_output_table_ids(project_id: str, dataset: str,
                          date_suffix: str) -> Mapping[str, str]:
  """Returns mapping from output table names to full BigQuery table ids.

  Args:
    project_id: Google Cloud Platform project id.
    dataset: Id of the dataset inside project_id to write the output tables.
    date_suffix: date_suffix string to append to the end of the tablenames.

  Returns:
    Mapping of table names to full BigQuery table ids.
    Format of returned id: <project>.<dataset>.<tablename>_<date_suffix>.
  """
  table_ids = {}
  for table in _OUTPUT_TABLES:
    table_ids[table] = _get_table_name(project_id, dataset, table, date_suffix)
  return table_ids


def _get_conversion_window_date_params(
    input_params: Mapping[str, Any]) -> Mapping[str, Any]:
  """Checks, transforms and returns conversion_window_date input_params.

  Args:
    input_params: Mapping from input parameter names to values.

  Returns:
    Mapping from conversion window date parameters to values.
  Raises:
    ValueError: User formatted message on error.
  """
  params = {}
  params['conversion_window_length'] = parse_int_param(
      input_params, 'conversion_window_length', 1)
  has_conversion_window_end_date = (
      input_params.get('conversion_window_end_date', None) is not None)
  has_conversion_window_end_today_offset_days = (
      input_params.get('conversion_window_end_today_offset_days', None) is
      not None)
  if (has_conversion_window_end_date ==
      has_conversion_window_end_today_offset_days):
    raise ValueError('Specify either conversion_window_end_date or '
                     'conversion_window_end_today_offset_days')
  # Compute the conversion window end date.
  end_date = None
  if has_conversion_window_end_today_offset_days:
    offset_days = parse_int_param(input_params,
                                  'conversion_window_end_today_offset_days', 0)
    end_date = (datetime.date.today() - datetime.timedelta(days=offset_days))
    params['conversion_window_end_date'] = end_date.isoformat()
  else:
    end_date = datetime.datetime.strptime(
        _get_param_or_die(input_params, 'conversion_window_end_date'),
        '%Y-%m-%d').date()
    if end_date > datetime.date.today():
      raise ValueError('conversion_window_end_date is in the future.')

  params['conversion_window_end_date'] = end_date.isoformat()
  start_date = end_date - datetime.timedelta(
      days=params['conversion_window_length'])
  params['conversion_window_start_date'] = start_date.isoformat()
  params['conversion_definition_sql'] = _strip_sql(
      jinja_env.get_template('conversion_definition.sql').render(params))
  return params


def _get_path_lookback_params(
    input_params: Mapping[str, Any]) -> Mapping[str, Any]:
  """Checks, transforms and returns path_lookback input_params.

  Args:
    input_params: Mapping from input parameter names to values.

  Returns:
    Mapping from path_lookback parameters to values.
  Raises:
    ValueError: User formatted message on error.
  """
  params = {}
  params['path_lookback_days'] = parse_int_param(input_params,
                                                 'path_lookback_days', 1)
  if input_params.get('path_lookback_steps', None) is None:
    params['path_lookback_steps'] = 0
  else:
    params['path_lookback_steps'] = parse_int_param(input_params,
                                                    'path_lookback_steps', 1)
  return params


def _extract_channels(client: bigquery.client.Client,
                      params: Mapping[str, Any]) -> List[str]:
  """Returns the list of names by running extract_channels.sql.

  Args:
    client: BigQuery client.
    params: Mapping of template parameter names to values.
  Returns:
    List of channel names.
  """
  extract_channels_sql = jinja_env.get_template('extract_channels.sql').render(
      params)
  channels = [
      row.channel for row in client.query(extract_channels_sql).result()]
  if fractribution.UNMATCHED_CHANNEL not in channels:
    channels.append(fractribution.UNMATCHED_CHANNEL)
  return channels


def _get_fullvisitorid_userid_map_params(
    input_params: Mapping[str, Any]) -> Mapping[str, Any]:
  """Checks, transforms and returns the userid-mapping input_params.

  Args:
    input_params: Mapping from input parameter names to values.

  Returns:
    Mapping from userid-mapping parameters to values.
  Raises:
    ValueError: User formatted message on error.
  """
  params = {}
  # Set the default behavior to update the id map.
  params['update_fullvisitorid_userid_map'] = input_params.get(
      'update_fullvisitorid_userid_map', True)
  # Extract the custom dimension containing the userid mapping
  if input_params.get('userid_ga_custom_dimension_index', None) is not None:
    params['userid_ga_custom_dimension_index'] = parse_int_param(
        input_params, 'userid_ga_custom_dimension_index', 1)
  else:
    params['userid_ga_custom_dimension_index'] = 0
  # Extract the hit-level custom dimension containing the userid mapping
  if input_params.get('userid_ga_hits_custom_dimension_index',
                      None) is not None:
    params['userid_ga_hits_custom_dimension_index'] = parse_int_param(
        input_params, 'userid_ga_hits_custom_dimension_index', 1)
  else:
    params['userid_ga_hits_custom_dimension_index'] = 0
  return params


def _get_template_params(input_params: Mapping[str, Any]) -> Dict[str, Any]:
  """Checks, transforms and returns input_params into an internal param mapping.

  Args:
    input_params: Mapping from input parameter names to values.

  Returns:
    Mapping of template parameter names to parameter values.
  Raises:
    ValueError: User formatted if input_params contains an error.
  """
  params = {}
  params['project_id'] = _get_param_or_die(input_params, 'project_id')
  params['dataset'] = _get_param_or_die(input_params, 'dataset')
  params['ga_sessions_table'] = _get_param_or_die(input_params,
                                                  'ga_sessions_table')
  if params['ga_sessions_table'][-2:] != '_*':
    raise ValueError('ga_sessions_table parameter must end in _*')
  params.update(_get_conversion_window_date_params(input_params))
  params.update(
      _get_output_table_ids(params['project_id'], params['dataset'],
                            params['conversion_window_end_date'])
  )  # TODO: isoformat for this date
  params.update(_get_path_lookback_params(input_params))
  params.update(_get_fullvisitorid_userid_map_params(input_params))
  # Process the hostname restrictions.
  if input_params.get('hostnames', None) is not None:
    params['hostnames'] = ', '.join([
        "'%s'" % hostname for hostname in input_params['hostnames'].split(',')
    ])
  # Check the path_transform.
  path_transform = _get_param_or_die(input_params, 'path_transform')
  if path_transform not in _PATH_TRANSFORMS_MAP.keys():
    raise ValueError(
        'Unknown path_transform. Use one of: ', _PATH_TRANSFORMS_MAP.keys())
  params['path_transforms'] = _PATH_TRANSFORMS_MAP[path_transform]
  return params


def extract_fractribution_input_data(client: bigquery.client.Client,
                                     params: Mapping[str, Any]) -> None:
  """Extracts the input data for fractribution into BigQuery.

  Args:
    client: BigQuery client.
    params: Mapping of all template parameter names to values.
  """
  extract_data_sql = _strip_sql(
      jinja_env.get_template('extract_data.sql').render(params))
  # Issue the query, and call result() to wait for it to finish. No results
  # are returned as all output is stored on BigQuery.
  client.query(extract_data_sql).result()


def run_fractribution(client: bigquery.client.Client,
                      params: Mapping[str, Any]) -> None:
  """Runs fractribution on the extract_fractribution_input_data BigQuery tables.

  Args:
    client: BigQuery client.
    params: Mapping of all template parameter names to values.
  """

  # Step 1: Extract the paths from the path_summary_table.
  frac = fractribution.Fractribution(
      client.query(
          jinja_env.get_template('select_path_summary_query.sql').render(
              path_summary_table=params['path_summary_table'])))
  # Step 2: Run Fractribution
  frac.run_fractribution()
  frac.normalize_channel_to_attribution_names()
  # Step 3: Create the path_summary_table and upload the results.
  create_path_summary_table_sql = jinja_env.get_template(
      'create_path_summary_results_table.sql').render(params)
  client.query(create_path_summary_table_sql).result()
  frac.upload_path_summary(client, params['path_summary_table'])


def generate_report(client: bigquery.client.Client,
                    params: Mapping[str, Any]) -> None:
  """Generates the final BigQuery Table with channel-level attribution and ROAS.

  Args:
    client: BigQuery client.
    params: Mapping of all template parameter names to values.
  """
  client.query(
      jinja_env.get_template('generate_report.sql').render(params)).result()


def run(input_params: Mapping[str, Any]) -> int:
  """Main entry point to run Fractribution with the given input_params.

  Args:
    input_params: Mapping from input parameter names to values.
  Returns:
    0 on success and non-zero otherwise
  """
  params = _get_template_params(input_params)
  client = bigquery.Client(params['project_id'])
  dataset = bigquery.Dataset('{}.{}'.format(params['project_id'],
                                            params['dataset']))
  if 'region' in params and params['region']:
    dataset.location = params['region']
  client.create_dataset(dataset, exists_ok=True)
  extract_fractribution_input_data(client, params)
  # Extract the channel definitions into params for use in later queries.
  params['channels'] = _extract_channels(client, params)
  run_fractribution(client, params)
  generate_report(client, params)
  return 0


def main(event, unused_context=None) -> int:
  """Entry point for Cloud Function."""
  input_params = json.loads(base64.b64decode(event['data']).decode('utf-8'))
  return run(input_params)


def standalone_main(_):
  input_params = FLAGS.flag_values_dict()
  run(input_params)


if __name__ == '__main__':
  app.run(standalone_main)
