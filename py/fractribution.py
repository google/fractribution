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


"""Library for computing fractional attribution."""

import io
import json
import re
from typing import Iterable, List, Tuple
from google.cloud import bigquery

# Default channel name when no match is found.
UNMATCHED_CHANNEL = 'Unmatched_Channel'


class _PathSummary(object):
  """Stores conversion and attribution information.

  To save space, the path itself is not stored here, as it is already stored
  as the key of the _path_tuple_to_summary dict in Fractribution.
  """

  def __init__(self, conversions: int, non_conversions: int, revenue: float):
    self.conversions = conversions
    self.non_conversions = non_conversions
    self.revenue = revenue
    self.channel_to_attribution = {}


class Fractribution(object):
  """Runs Fractribution on a set of marketing paths to (non-)conversion."""

  @classmethod
  def _get_path_string(cls, path_tuple: Iterable[str]) -> str:
    return ' > '.join(path_tuple)

  def __init__(self, query_job: bigquery.job.QueryJob):
    """Loads (path_str, conversions, non_conversions, revenue) from query_job.

    Args:
      query_job: QueryJob of (path_str, conversions, non_conversions, revenue).
    """
    self._path_tuple_to_summary = {}
    for (path_str, conversions, non_conversions, revenue) in query_job:
      path_tuple = ()
      if path_str:
        path_tuple = tuple(path_str.split(' > '))
      if path_tuple not in self._path_tuple_to_summary:
        self._path_tuple_to_summary[path_tuple] = _PathSummary(
            conversions, non_conversions, revenue)
      else:
        path_summary = self._path_tuple_to_summary[path_tuple]
        path_summary.conversions += conversions
        path_summary.non_conversions += non_conversions

  def _get_conversion_probability(
      self, path_tuple: Tuple[str, ...]) -> float:
    """Returns path_tuple conversion/(conversion+non_conversion) probability.

    Args:
      path_tuple: Tuple of channel names in the path.

    Returns:
      Conversion probability of customers with this path.
    """

    if path_tuple not in self._path_tuple_to_summary:
      return 0.0
    path_summary = self._path_tuple_to_summary[path_tuple]
    count = path_summary.conversions + path_summary.non_conversions
    if not count:
      return 0.0
    return path_summary.conversions / count

  def _get_counterfactual_marginal_contributions(
      self, path_tuple: Tuple[str, ...]) -> List[float]:
    """Returns the marginal contribution of each channel in the path.

    Args:
      path_tuple: Tuple of channel names in the path.

    Returns:
      List of marginal contribution values, one for each channel in path_tuple.
    """
    if not path_tuple:
      return []
    marginal_contributions = [0] * len(path_tuple)
    path_conversion_probability = self._get_conversion_probability(path_tuple)
    # If the path contains a single channel, it gets 100% of the contribution.
    if len(path_tuple) == 1:
      marginal_contributions[0] = path_conversion_probability
    else:
      # Otherwise, compute the counterfactual marginal contributions by channel.
      for i in range(len(path_tuple)):
        counterfactual_tuple = path_tuple[:i] + path_tuple[i+1:]
        raw_marginal_contribution = (
            path_conversion_probability -
            self._get_conversion_probability(counterfactual_tuple))
        # Avoid negative contributions by flooring to 0.
        marginal_contributions[i] = max(raw_marginal_contribution, 0)
    return marginal_contributions

  def run_fractribution(self, normalize: bool = True) -> None:
    """Compute fractional attribution values for all given paths.

    Side-effect: Updates channel_to_attribution dicts in _path_tuple_to_summary.

    Args:
      normalize: Set to True to normalize marginal contribution values and False
          otherwise. Default: True
    """
    for path_tuple, path_summary in self._path_tuple_to_summary.items():
      # Ignore empty paths, which can happen when there is a conversion, but
      # no matching marketing channel events. Also ignore paths with no
      # conversions, since there is no attribution to make.
      if not path_tuple or not path_summary.conversions:
        continue
      path_summary.channel_to_attribution = {}
      marginal_contributions = self._get_counterfactual_marginal_contributions(
          path_tuple)
      sum_marginal_contributions = sum(marginal_contributions)
      if normalize and sum_marginal_contributions:
        marginal_contributions = [
            marginal_contribution / sum_marginal_contributions
            for marginal_contribution in marginal_contributions]
      # Use last touch attribution if no channel has a marginal_contribution.
      if sum_marginal_contributions == 0:
        marginal_contributions[-1] = 1
      # Aggregate the marginal contributions by channel, as channels can occur
      # more than once in the path.
      for i, channel in enumerate(path_tuple):
        path_summary.channel_to_attribution[channel] = (
            marginal_contributions[i]
            + path_summary.channel_to_attribution.get(channel, 0.0))

  def run_last_touch_attribution(self) -> None:
    """Assigns 100% attribution to the last channel in each path.

    Side-effect: Updates channel_to_attribution dicts in _path_tuple_to_summary.
    """
    for path_tuple, path_summary in self._path_tuple_to_summary.items():
      path_summary.channel_to_attribution = {}
      for channel in path_tuple:
        path_summary.channel_to_attribution[channel] = 0.0
      if path_tuple:
        path_summary.channel_to_attribution[path_tuple[-1]] = 1

  def normalize_channel_to_attribution_names(self) -> None:
    """Normalizes channel names and aggregates attribution values if necessary.

    Path transforms can also transform channel names to include a count
    related suffix (<COUNT>). This function undoes the transform on the channel
    name by removing the suffix, so that a single channel with two different
    suffixes can be aggregated.

    Side-effect: Updates channel_to_attribution names in _path_tuple_to_summary.
    """
    for path_summary in self._path_tuple_to_summary.values():
      channel_to_attribution = {}
      for channel in path_summary.channel_to_attribution:
        normalized_channel = re.sub(r'\(.*', '', channel)
        channel_to_attribution[normalized_channel] = (
            channel_to_attribution.get(normalized_channel, 0) +
            path_summary.channel_to_attribution[channel])
      path_summary.channel_to_attribution = channel_to_attribution

  def _path_summary_to_json_stringio(self) -> io.StringIO:
    """Returns a StringIO file with one JSON-encoded _PathSummary per line."""

    default_attribution = {UNMATCHED_CHANNEL: 1.0}
    stringio = io.StringIO()
    for path_tuple, path_summary in self._path_tuple_to_summary.items():
      row = {'transformedPath': self._get_path_string(path_tuple),
             'conversions': path_summary.conversions,
             'nonConversions': path_summary.non_conversions,
             'revenue': path_summary.revenue}
      if path_summary.channel_to_attribution:
        row.update(path_summary.channel_to_attribution)
      else:
        row.update(default_attribution)
      stringio.write(json.dumps(row))
      stringio.write('\n')
    stringio.flush()
    stringio.seek(0)
    return stringio

  def upload_path_summary(
      self, client: bigquery.client.Client, path_summary_table: str) -> None:
    """Uploads the path summary data to the given path_summary_table.

    Args:
      client: BigQuery Client
      path_summary_table: Name of the table to write the path summaries.
    """
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = False
    job = client.load_table_from_file(
        self._path_summary_to_json_stringio(),
        client.get_table(path_summary_table),
        job_config=job_config)
    job.result()  # Waits for table load to complete.
