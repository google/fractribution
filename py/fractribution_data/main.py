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


"""Loads the data into BigQuery needed to run Fractribution."""

import base64
import datetime
import distutils.util
import json
import logging
import os
import re

from jinja2 import Environment
from jinja2 import FileSystemLoader

from google.cloud import bigquery
from google.cloud import pubsub_v1

_CORE_FULLVISITORID_CUSTOMER_ID_MAP_TABLE_SCHEMA = [
    bigquery.SchemaField('fullVisitorId', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('customer_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('mapping_visitStartTime', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('added_at_end_suffix', 'STRING', mode='REQUIRED')
]


_ADDITIONAL_REMOVAL_METHODS = [
    'none', 'upload_customer_ids', 'previously_known_customer_ids']

env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(__file__), 'templates')))


def strip_comments_from_sql(sql: str) -> str:
  """Returns the given sql string with comments removed.

  Args:
    sql: A SQL command string
  Returns:
    The given sql string, where for each line, anything substring from '#'
     or '--' onwards is removed.
  """
  lines = []
  for line in sql.split('\n'):
    line = re.sub(r'\s*(#|--).*', '', line)
    if line:
      lines.append(line)
  return '\n'.join(lines)


def run_query_with_destination(
    client: bigquery.Client, sql: str, destination: str,
    write_disposition: bigquery.WriteDisposition =
    bigquery.WriteDisposition.WRITE_TRUNCATE) -> bigquery.table.RowIterator:
  """Runs the given sql command, writing the results to the given destination.

  Args:
    client: BigQuery client run the sql command on.
    sql: SQL command to run.
    destination: BigQuery table name to write the results.
    write_disposition: Whether to overwrite or append the results.
  Returns:
    bigquery.table.RowIterator over the results.
  """
  logging.info('Running sql query: %s', strip_comments_from_sql(sql))
  job_config = bigquery.QueryJobConfig()
  job_config.create_disposition = (
      bigquery.job.CreateDisposition.CREATE_IF_NEEDED)
  job_config.write_disposition = write_disposition
  job_config.destination = destination
  job_config.allow_large_results = True
  query_job = client.query(strip_comments_from_sql(sql), job_config=job_config)
  result = query_job.result()
  logging.info('Query successful')
  return result



def get_flag_or_die(flags: dict, flag: str) -> str:
  """Returns the value of the given flag. Dies if the flag is not defined.

  Args:
    flags: Dictionary of (flag name, flag value)
    flag: Target flag to get the value of.
  Returns:
    Value of the given flag as a string.
  Raises:
    ValueError: User formatted error message if the flag is not defined.
  """
  try:
    return flags[flag]
  except KeyError:
    raise ValueError('Missing flag: %s' % flag)


class PrepareInputForFractribution(object):
  """Prepares the BigQuery table inputs for Fractribution."""


  def __init__(self, data: dict):
    """Initialize PrepareInputForFractribution.

    Args:
      data: Dictionary from flag name to flag value.
    """
    self._data = data

    self._project_id = get_flag_or_die(data, 'project_id')
    # Construct a BigQuery client for this project_id
    self._client = bigquery.Client(project=self._project_id)

    self._ga_sessions_table = get_flag_or_die(data, 'ga_sessions_table')
    self._channel_definitions = env.get_template(
        get_flag_or_die(data, 'channel_definitions')).render()
    self._hostnames = data.get('hostnames', '')
    if self._hostnames:
      self._hostnames = ', '.join(
          ["'%s'" % hostname for hostname in self._hostnames.split(',')])

    # Intermediate/Output table names.
    self._target_endpoints_table = get_flag_or_die(
        data, 'target_endpoints_table')
    self._session_event_log_table = get_flag_or_die(
        data, 'session_event_log_table')
    self._paths_to_conversion_table = get_flag_or_die(
        data, 'paths_to_conversion_table')
    self._paths_to_non_conversion_table = get_flag_or_die(
        data, 'paths_to_non_conversion_table')
    self._path_summary_table = get_flag_or_die(data, 'path_summary_table')
    self._channel_counts_table = get_flag_or_die(data, 'channel_counts_table')
    self._core_fullvisitorid_customer_id_map_table = data.get(
        'core_fullvisitorid_customer_id_map_table', None)
    if self._core_fullvisitorid_customer_id_map_table:
      self._client.create_table(
          bigquery.Table(
              self._core_fullvisitorid_customer_id_map_table,
              schema=_CORE_FULLVISITORID_CUSTOMER_ID_MAP_TABLE_SCHEMA),
          exists_ok=True)

    # Report window flags.
    self._report_window_start = get_flag_or_die(data, 'report_window_start')
    self._parsed_start = datetime.date.fromisoformat(self._report_window_start)  # pytype: disable=attribute-error
    self._report_window_end = get_flag_or_die(data, 'report_window_end')
    self._parsed_end = datetime.date.fromisoformat(self._report_window_end)  # pytype: disable=attribute-error
    # Stop if report_window_end is in the future
    if self._parsed_end > datetime.date.today():
      raise ValueError(
          'report_window_end=%s in the future' % self._report_window_end)
    # Stop if report_window_end is before report_window_start.
    if self._parsed_end < self._parsed_start:
      raise ValueError('report_window_end=%s is before report_window_start=%s' %
                       (self._report_window_start, self._report_window_end))

    # Lookback flags.
    # Both lookback_days and lookback_steps must be positive integers.
    self._lookback_days = int(data.get('lookback_days', 0))
    if self._lookback_days < 0:
      raise ValueError(
          'lookback_days=%d is less than zero' % self._lookback_days)
    self._lookback_steps = int(data.get('lookback_steps', 0))
    if self._lookback_steps < 0:
      raise ValueError(
          'lookback_steps=%d is less than zero' % self._lookback_steps)

    # Flags related to top_up_core
    self._top_up_core = distutils.util.strtobool(
        data.get('top_up_core', 'False'))
    self._id_ga_custom_dimension_id = int(
        data.get('id_ga_custom_dimension_id', -1))
    self._id_regex_filter = data.get('id_regex_filter', '')
    if self._top_up_core:
      if not self._id_ga_custom_dimension_id < 0:
        raise ValueError(
            'Must supply id_ga_custom_dimension_id when top_up_core=True')
      if not self._core_fullvisitorid_customer_id_map_table:
        raise ValueError(
            'Must supply core_fullvisitorid_customer_id_map_table '
            'when top_up_core=True')

    # Additional removal flags.
    self._additional_removals_method = data.get(
        'additional_removals_method', 'none')
    if self._additional_removals_method not in _ADDITIONAL_REMOVAL_METHODS:
      raise ValueError('Invalid additional_removals_method: %s' %
                       self._additional_removals_method)

    # Other.
    self._augment_empty_conversion_paths = False
    self._converted_customers_table = self._paths_to_conversion_table

  def _create_target_endpoints_table(self):
    """Create the self._target_endpoints table."""
    raise NotImplementedError('Must implement _create_target_endpoints_table()')

  def _get_session_event_log_table_template_values(self):
    # Override this function to add endpoint specific template values.
    #
    # Note that the GA session tables are partitioned in local time, while dates
    # within a GA are in UTC. To extract all GA sessions, we extend the table
    # suffix dates to one day either side of the given report date range.
    return {
        'report_window_start': self._report_window_start,
        'report_window_end': self._report_window_end,
        'visit_filter_start': datetime.date.isoformat(
            self._parsed_start - datetime.timedelta(days=self._lookback_days)),
        'suffix_filter_start': datetime.date.strftime(
            self._parsed_start - datetime.timedelta(
                days=self._lookback_days+1), '%Y%m%d'),
        'suffix_filter_end': datetime.date.strftime(
            self._parsed_end + datetime.timedelta(days=1), '%Y%m%d'),
        'channel_defn': self._channel_definitions,
        'ga_sessions_table': self._ga_sessions_table,
        'hostnames': self._hostnames,
        'additional_removals_method': self._additional_removals_method
    }

  def _create_session_event_log_table(self):
    template_values = self._get_session_event_log_table_template_values()
    run_query_with_destination(
        client=self._client,
        sql=env.get_template('session_event_log.sql').render(**template_values),
        destination=self._session_event_log_table)

  def _create_paths_to_conversion_table(self):
    run_query_with_destination(
        client=self._client,
        sql=env.get_template('paths_to_conversion.sql').render(
            target_endpoints_table=self._target_endpoints_table,
            session_event_log_table=self._session_event_log_table,
            lookback_days=self._lookback_days,
            lookback_steps=self._lookback_steps,
            augment_empty_conversion_paths=(
                self._augment_empty_conversion_paths)),
        destination=self._paths_to_conversion_table)

  def _create_paths_to_non_conversion_table(self):
    run_query_with_destination(
        client=self._client,
        sql=env.get_template('paths_to_non_conversion.sql').render(
            converted_customers_table=self._converted_customers_table,
            session_event_log_table=self._session_event_log_table,
            lookback_days=self._lookback_days,
            lookback_steps=self._lookback_steps),
        destination=self._paths_to_non_conversion_table)

  def _create_path_summary(self):
    run_query_with_destination(
        client=self._client,
        sql=env.get_template('path_summary.sql').render(
            paths_to_conversion_table=self._paths_to_conversion_table,
            paths_to_non_conversion_table=self._paths_to_non_conversion_table),
        destination=self._path_summary_table)

  def _create_channel_counts(self):
    run_query_with_destination(
        client=self._client,
        sql=env.get_template('channel_counts.sql').render(
            session_event_log_table=self._session_event_log_table),
        destination=self._channel_counts_table)

  def _top_up_core_fullvisitorid_customer_id_map_table(self):
    run_query_with_destination(
        client=self._client,
        sql=env.get_template('core_fullvisitorid_customer_id_map.sql').render(
            suffix_filter_end=datetime.date.strftime(
                datetime.date.today() - datetime.timedelta(days=1), '%Y%m%d'),
            ga_sessions_table=self._ga_sessions_table,
            id_ga_custom_dimension_id=self._id_ga_custom_dimension_id,
            id_regex_filter=self._id_regex_filter,
            core_fullvisitorid_customer_id_map_table=(
                self._core_fullvisitorid_customer_id_map_table)),
        destination=self._core_fullvisitorid_customer_id_map_table)

  def run(self):
    self._create_target_endpoints_table()
    self._create_session_event_log_table()
    self._create_paths_to_conversion_table()
    self._create_paths_to_non_conversion_table()
    self._create_channel_counts()
    self._create_path_summary()
    if self._top_up_core:
      self._top_up_core_fullvisitorid_customer_id_map_table()


class PrepareInputForFractributionCustomEndpoint(PrepareInputForFractribution):
  """Prepares table inputs for Fractribution with custom endpoint definition."""

  def __init__(self, data: dict):
    """Initialize PrepareInputForFractributionCustomEndpoint.

    Args:
      data: Dictionary from flag name to flag value.
    """
    super().__init__(data)
    self._endpoint_definition = env.get_template(
        data.get('endpoint_definition',
                 'templates/custom_endpoint_definition_example.sql')).render()
    if self._additional_removals_method != 'none':
      raise ValueError(
          'Unsupported flag value: additional_removals_method=%s' %
          self._additional_removals_method)
    self._augment_empty_conversion_paths = False

  def _create_target_endpoints_table(self):
    run_query_with_destination(
        client=self._client,
        sql=env.get_template(
            'target_endpoints_with_endpoint_definition.sql').render(
                ga_sessions_table=self._ga_sessions_table,
                endpoint_definition=strip_comments_from_sql(
                    self._endpoint_definition),
                visit_filter_start=self._report_window_start,
                visit_filter_end=self._report_window_end,
                suffix_filter_start=datetime.date.strftime(
                    self._parsed_start - datetime.timedelta(days=1), '%Y%m%d'),
                suffix_filter_end=datetime.date.strftime(
                    self._parsed_end + datetime.timedelta(days=1), '%Y%m%d')),
        destination=self._target_endpoints_table)


class PrepareInputForFractributionUploadEndpoint(PrepareInputForFractribution):
  """Prepares table inputs for Fractribution with upload endpoint definition."""

  def __init__(self, data: dict):
    """Initialize PrepareInputForFractributionUploadEndpoint.

    Args:
      data: Dictionary from flag name to flag value.
    """
    super().__init__(data)

    # Name of the BigQuery table containing endpoints. This table must have the
    # columns:
    #   customer_id: Your internal id for the customer.
    #   endpoint_datetime: UTC timestamp of the endpoint in format
    #       'yyyy-mm-dd hh:mm:ss UTC'.
    self._endpoint_upload_bq_table = get_flag_or_die(
        data, 'endpoint_upload_bq_table')
    self._core_fullvisitorid_customer_id_map_table = get_flag_or_die(
        data, 'core_fullvisitorid_customer_id_map_table')
    self._target_customer_id_map_matches_table = get_flag_or_die(
        data, 'target_customer_id_map_matches_table')

    # Additional removals definitions
    if (self._additional_removals_method in (
        'previously_known_customer_ids', 'upload_customer_ids')
        and not self._core_fullvisitorid_customer_id_map_table):
      raise ValueError(
          'Must supply core_fullvisitorid_customer_id_map_table when '
          'additional_removals_method in '
          '(previously_known_customer_ids, upload_customer_ids).')
    # Name of the BigQuery table containing customers to ignore. The table must
    # have two columns:
    #   customer_id: Your internal id for the customer.
    #   reference_date: 'yyyy-mm-dd' string. Ignore customer if this date is
    #       within the report window.
    self._additional_removals_bq_table = data.get(
        'additional_removals_bq_table', None)
    if (self._additional_removals_method == 'upload_customer_ids'
        and not self._additional_removals_bq_table):
      raise ValueError(
          'Must supply additional_removals_bq_table when '
          'additional_removals_method=upload_customer_ids.')
    self._max_map_minutes = int(data.get('max_map_minutes', 20))

    # Attempt to augment empty conversion paths with events in the future.
    self._augment_empty_conversion_paths = True
    self._converted_customers_table = self._target_customer_id_map_matches_table

  def _create_target_endpoints_table(self):
    run_query_with_destination(
        client=self._client,
        sql=env.get_template('target_customer_id_map_matches.sql').render(
            endpoint_upload_bq_table=self._endpoint_upload_bq_table,
            report_window_start=self._report_window_start,
            report_window_end=self._report_window_end,
            core_fullvisitorid_customer_id_map_table=(
                self._core_fullvisitorid_customer_id_map_table)),
        destination=self._target_customer_id_map_matches_table)
    run_query_with_destination(
        client=self._client,
        sql=env.get_template(
            'target_endpoints_with_endpoint_bq_upload.sql').render(
                target_customer_id_map_matches_table=(
                    self._target_customer_id_map_matches_table),
                max_map_minutes=self._max_map_minutes),
        destination=self._target_endpoints_table)

  def _get_session_event_log_table_template_values(self):
    template_values = super()._get_session_event_log_table_template_values()
    template_values.update({
        'additional_removals_table': self._additional_removals_bq_table,
        'core_fullvisitorid_customer_id_map_table':
            self._core_fullvisitorid_customer_id_map_table
    })
    return template_values


def set_report_window(data):
  """Sets the report date parameters if they are missing.

  Args:
    data: Dictionary of parameters
  """
  date_format = '%Y-%m-%d'

  if 'report_window_end_offset_from_currdate' in data:
    offset = get_flag_or_die(data, 'report_window_end_offset_from_currdate')
    end_date = datetime.date.today() - datetime.timedelta(days=int(offset))
    data['report_window_end'] = end_date.strftime(date_format)

  if 'report_window_length' in data:
    end_date = datetime.datetime.strptime(data['report_window_end'],
                                          date_format)
    date_period = get_flag_or_die(data, 'report_window_length')
    start_date = end_date - datetime.timedelta(days=int(date_period))
    data['report_window_start'] = start_date.strftime(date_format)


def trigger_fractribution_model(data):
  """Sends pub/sub message to trigger Fractribution Model.

  Args:
    data: Dictionary of parameters
  """
  if 'model_topic_name' in data:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        get_flag_or_die(data, 'project_id'),
        get_flag_or_die(data, 'model_topic_name'))
    message = json.dumps(data).encode('utf-8')
    future = publisher.publish(topic_path, message)
    future.result()


def prepare_input_for_fractribution_custom_endpoint(event, unused_context):
  """Entry point for Cloud Function for custom endpoint."""

  data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
  set_report_window(data)
  PrepareInputForFractributionCustomEndpoint(data).run()
  trigger_fractribution_model(data)

  # Return explicit 0 for success to force termination of the background Cloud
  # Function. If there was failure, an exception/error would have already been
  # thrown.
  return 0


def prepare_input_for_fractribution_upload_endpoint(event, unused_context):
  """Entry point for Cloud Function for upload endpoint."""

  data = json.loads(base64.b64decode(event['data']).decode('utf-8'))
  set_report_window(data)
  PrepareInputForFractributionUploadEndpoint(data).run()
  trigger_fractribution_model(data)

  # Return explicit 0 for success to force termination of the background Cloud
  # Function. If there was failure, an exception/error would have already been
  # thrown.
  return 0
